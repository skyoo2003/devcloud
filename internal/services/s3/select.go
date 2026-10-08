// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

type selectRequestXML struct {
	XMLName            xml.Name `xml:"SelectObjectContentRequest"`
	Expression         string   `xml:"Expression"`
	ExpressionType     string   `xml:"ExpressionType"`
	InputSerialization struct {
		CSV *struct {
			FileHeaderInfo string `xml:"FileHeaderInfo"`
		} `xml:"CSV"`
		JSON *struct {
			Type string `xml:"Type"`
		} `xml:"JSON"`
	} `xml:"InputSerialization"`
	OutputSerialization struct {
		CSV  *struct{} `xml:"CSV"`
		JSON *struct{} `xml:"JSON"`
	} `xml:"OutputSerialization"`
}

func encodeEventMessage(headers map[string]string, payload []byte) []byte {
	var headerBytes bytes.Buffer
	for k, v := range headers {
		kBytes := []byte(k)
		vBytes := []byte(v)
		headerBytes.WriteByte(byte(len(kBytes)))
		headerBytes.Write(kBytes)
		headerBytes.WriteByte(7) // 7 = String type in AWS EventStream
		var vLen [2]byte
		binary.BigEndian.PutUint16(vLen[:], uint16(len(vBytes)))
		headerBytes.Write(vLen[:])
		headerBytes.Write(vBytes)
	}

	headersLen := headerBytes.Len()
	payloadLen := len(payload)
	totalLen := 12 + headersLen + payloadLen + 4

	var preludeCore [8]byte
	binary.BigEndian.PutUint32(preludeCore[0:4], uint32(totalLen))
	binary.BigEndian.PutUint32(preludeCore[4:8], uint32(headersLen))
	preludeCRC := crc32.ChecksumIEEE(preludeCore[:])

	msg := make([]byte, 0, totalLen)
	msg = append(msg, preludeCore[:]...)
	var preludeCRCBits [4]byte
	binary.BigEndian.PutUint32(preludeCRCBits[:], preludeCRC)
	msg = append(msg, preludeCRCBits[:]...)
	msg = append(msg, headerBytes.Bytes()...)
	msg = append(msg, payload...)

	msgCRC := crc32.ChecksumIEEE(msg)
	var msgCRCBits [4]byte
	binary.BigEndian.PutUint32(msgCRCBits[:], msgCRC)
	msg = append(msg, msgCRCBits[:]...)
	return msg
}

func (p *S3Provider) selectObjectContent(_ context.Context, bucket, key string, req *http.Request) (*plugin.Response, error) {
	if bucket == "" || key == "" {
		return xmlError("InvalidRequest", "bucket and key required", http.StatusBadRequest), nil
	}

	// Verify object exists
	_, err := p.metaStore.GetObjectMeta(bucket, key, defaultAccountID)
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			return xmlError("NoSuchKey", fmt.Sprintf("key %q not found", key), http.StatusNotFound), nil
		}
		return nil, err
	}

	data, err := p.fileStore.GetObject(defaultAccountID, bucket, key)
	if err != nil {
		return nil, err
	}

	var selReq selectRequestXML
	if req.Body != nil {
		body, err := io.ReadAll(req.Body)
		if err == nil && len(body) > 0 {
			_ = xml.Unmarshal(body, &selReq)
		}
	}

	records := executeSelectQuery(data, selReq)

	var stream bytes.Buffer
	// 1. Records event
	recordsHeader := map[string]string{
		":message-type": "event",
		":event-type":   "Records",
		":content-type": "application/octet-stream",
	}
	stream.Write(encodeEventMessage(recordsHeader, records))

	// 2. Stats event
	statsPayload := fmt.Sprintf(
		`<Stats><BytesScanned>%d</BytesScanned><BytesProcessed>%d</BytesProcessed><BytesReturned>%d</BytesReturned></Stats>`,
		len(data), len(data), len(records),
	)
	statsHeader := map[string]string{
		":message-type": "event",
		":event-type":   "Stats",
		":content-type": "text/xml",
	}
	stream.Write(encodeEventMessage(statsHeader, []byte(statsPayload)))

	// 3. End event
	endHeader := map[string]string{
		":message-type": "event",
		":event-type":   "End",
	}
	stream.Write(encodeEventMessage(endHeader, nil))

	return &plugin.Response{
		StatusCode:  http.StatusOK,
		ContentType: "application/octet-stream",
		Body:        stream.Bytes(),
	}, nil
}

func executeSelectQuery(data []byte, selReq selectRequestXML) []byte {
	expr := strings.TrimSpace(selReq.Expression)
	whereClause := ""
	if idx := strings.Index(strings.ToUpper(expr), " WHERE "); idx != -1 {
		whereClause = strings.TrimSpace(expr[idx+7:])
	}

	isJSON := selReq.InputSerialization.JSON != nil
	if isJSON {
		return queryJSON(data, whereClause)
	}
	return queryCSV(data, whereClause, selReq)
}

func queryCSV(data []byte, whereClause string, selReq selectRequestXML) []byte {
	r := csv.NewReader(bytes.NewReader(data))
	r.FieldsPerRecord = -1
	records, err := r.ReadAll()
	if err != nil {
		// Fallback to raw lines if CSV parsing fails
		return data
	}
	if len(records) == 0 {
		return nil
	}

	var out bytes.Buffer
	w := csv.NewWriter(&out)

	startIndex := 0
	hasHeader := selReq.InputSerialization.CSV != nil &&
		strings.EqualFold(selReq.InputSerialization.CSV.FileHeaderInfo, "USE")
	var headers []string
	if hasHeader && len(records) > 0 {
		headers = records[0]
		startIndex = 1
	}

	// Simple equality check parser: e.g. "s.id = '1'" or "id = '1'" or "s._1 = '1'"
	var filterCol string
	var filterVal string
	if whereClause != "" {
		parts := strings.SplitN(whereClause, "=", 2)
		if len(parts) == 2 {
			col := strings.TrimSpace(parts[0])
			val := strings.Trim(strings.TrimSpace(parts[1]), "'\"")
			col = strings.TrimPrefix(col, "s.")
			filterCol = col
			filterVal = val
		}
	}

	for i := startIndex; i < len(records); i++ {
		row := records[i]
		if filterCol != "" {
			match := false
			// Check by positional index _1, _2...
			if strings.HasPrefix(filterCol, "_") {
				var idx int
				if _, err := fmt.Sscanf(filterCol, "_%d", &idx); err == nil && idx >= 1 && idx <= len(row) {
					if row[idx-1] == filterVal {
						match = true
					}
				}
			}
			// Check by column header name
			if !match && len(headers) == len(row) {
				for hIdx, hName := range headers {
					if strings.EqualFold(hName, filterCol) && row[hIdx] == filterVal {
						match = true
						break
					}
				}
			}
			if !match {
				continue
			}
		}
		_ = w.Write(row)
	}
	w.Flush()
	return out.Bytes()
}

func queryJSON(data []byte, whereClause string) []byte {
	lines := strings.Split(string(data), "\n")
	var matched []string

	var filterKey string
	var filterVal string
	if whereClause != "" {
		parts := strings.SplitN(whereClause, "=", 2)
		if len(parts) == 2 {
			col := strings.TrimSpace(parts[0])
			val := strings.Trim(strings.TrimSpace(parts[1]), "'\"")
			col = strings.TrimPrefix(col, "s.")
			filterKey = col
			filterVal = val
		}
	}

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if filterKey != "" {
			var obj map[string]any
			if err := json.Unmarshal([]byte(line), &obj); err == nil {
				if v, ok := obj[filterKey]; ok {
					if fmt.Sprint(v) == filterVal {
						matched = append(matched, line)
					}
				}
				continue
			}
		}
		matched = append(matched, line)
	}

	if len(matched) == 0 {
		return nil
	}
	return []byte(strings.Join(matched, "\n") + "\n")
}
