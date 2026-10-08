// SPDX-License-Identifier: Apache-2.0
package sns

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"golang.org/x/crypto/pbkdf2"
)

type SandboxPhone struct {
	PhoneNumber, AccountID, LanguageCode, Status, OTPHash string
	CreatedAt, ExpiresAt                                  time.Time
	Consumed                                              bool
}

const (
	otpPBKDF2Iterations = 10000
	otpPBKDF2KeyLen     = 32
)

func deriveOTPHash(otp string, salt []byte) string {
	key := pbkdf2.Key([]byte(otp), salt, otpPBKDF2Iterations, otpPBKDF2KeyLen, sha256.New)
	return hex.EncodeToString(salt) + ":" + hex.EncodeToString(key)
}

func verifyOTPHash(storedHash, otp string) bool {
	saltHex, keyHex, ok := strings.Cut(storedHash, ":")
	if !ok {
		h := sha256.Sum256([]byte(otp))
		return subtle.ConstantTimeCompare([]byte(storedHash), []byte(hex.EncodeToString(h[:]))) == 1
	}
	salt, err := hex.DecodeString(saltHex)
	if err != nil {
		return false
	}
	expectedKey, err := hex.DecodeString(keyHex)
	if err != nil {
		return false
	}
	computedKey := pbkdf2.Key([]byte(otp), salt, otpPBKDF2Iterations, otpPBKDF2KeyLen, sha256.New)
	return subtle.ConstantTimeCompare(expectedKey, computedKey) == 1
}

func (s *SNSStore) IssueSandboxChallenge(phone, accountID, language string, now time.Time) (*SandboxPhone, error) {
	if e := validateSandboxPhone(phone); e != nil {
		return nil, e
	}
	if !sandboxLanguages[language] {
		return nil, invalidSNS("invalid LanguageCode")
	}
	var out *SandboxPhone
	e := s.withStateTx(func(tx *sql.Tx) error {
		var n int
		if e := tx.QueryRow(`SELECT COUNT(*) FROM sms_opt_outs WHERE phone_number=? AND account_id=?`, phone, accountID).Scan(&n); e != nil {
			return e
		}
		if n != 0 {
			return invalidSNS("phone number is opted out")
		}
		previous, e := snsDocument[SandboxPhone](tx, `SELECT document_json FROM sns_sandbox_phones WHERE account_id=? AND phone_number=?`, ErrSandboxPhoneNotFound, accountID, phone)
		if e != nil && !errors.Is(e, ErrSandboxPhoneNotFound) {
			return e
		}
		otp, hash := "", ""
		salt := make([]byte, 16)
		for {
			n, e := rand.Int(rand.Reader, big.NewInt(1000000))
			if e != nil {
				return e
			}
			if _, e := rand.Read(salt); e != nil {
				return e
			}
			otp = fmt.Sprintf("%06d", n.Int64())
			hash = deriveOTPHash(otp, salt)
			if previous == nil || !verifyOTPHash(previous.OTPHash, otp) {
				break
			}
		}
		v := SandboxPhone{PhoneNumber: phone, AccountID: accountID, LanguageCode: language, Status: "UNVERIFIED", OTPHash: hash, CreatedAt: now, ExpiresAt: now.Add(10 * time.Minute)}
		if previous != nil {
			v.CreatedAt = previous.CreatedAt
		}
		raw, e := json.Marshal(v)
		if e != nil {
			return e
		}
		body, e := json.Marshal(map[string]string{"PhoneNumber": phone, "OneTimePassword": otp, "LanguageCode": language, "Type": "verification"})
		if e != nil {
			return e
		}
		_, e = tx.Exec(`INSERT INTO sns_sandbox_phones VALUES (?,?,?) ON CONFLICT(account_id,phone_number) DO UPDATE SET document_json=excluded.document_json`, accountID, phone, raw)
		if e != nil {
			return e
		}
		_, e = tx.Exec(`INSERT INTO sns_sms_outbox VALUES (?,?,?,?,?)`, randomID(16), accountID, phone, body, now.Unix())
		if e == nil {
			out = &v
		}
		return e
	})
	return out, e
}

func (s *SNSStore) VerifySandboxChallenge(phone, accountID, otp string, now time.Time) error {
	if e := validateSandboxPhone(phone); e != nil {
		return e
	}
	if !sandboxOTP.MatchString(otp) {
		return invalidSNS("OneTimePassword must be 5–8 digits")
	}
	return s.withStateTx(func(tx *sql.Tx) error {
		v, e := snsDocument[SandboxPhone](tx, `SELECT document_json FROM sns_sandbox_phones WHERE account_id=? AND phone_number=?`, ErrSandboxPhoneNotFound, accountID, phone)
		if e != nil {
			return e
		}
		if v.Consumed || !now.Before(v.ExpiresAt) || v.OTPHash == "" || !verifyOTPHash(v.OTPHash, otp) {
			return ErrOTPVerification
		}
		v.Status = "VERIFIED"
		v.Consumed = true
		raw, e := json.Marshal(v)
		if e != nil {
			return e
		}
		_, e = tx.Exec(`UPDATE sns_sandbox_phones SET document_json=? WHERE account_id=? AND phone_number=?`, raw, accountID, phone)
		return e
	})
}
func (s *SNSStore) ListSandboxPhones(accountID string) ([]SandboxPhone, error) {
	rows, e := s.store.DB().Query(`SELECT document_json FROM sns_sandbox_phones WHERE account_id=? ORDER BY phone_number`, accountID)
	return listSNSDocuments[SandboxPhone](rows, e)
}
func (s *SNSStore) DeleteSandboxPhone(phone, accountID string) error {
	if e := validateSandboxPhone(phone); e != nil {
		return e
	}
	return s.withStateTx(func(tx *sql.Tx) error {
		_, e := tx.Exec(`DELETE FROM sns_sandbox_phones WHERE account_id=? AND phone_number=?`, accountID, phone)
		return e
	})
}
