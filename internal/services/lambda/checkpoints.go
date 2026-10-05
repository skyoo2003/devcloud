// SPDX-License-Identifier: Apache-2.0
package lambda

import (
	"database/sql"
	"errors"
)

type StreamCheckpoint struct {
	Generation     string
	SequenceNumber string
}

func (s *LambdaStore) GetStreamCheckpoint(mappingUUID, shardID string) (*StreamCheckpoint, error) {
	var c StreamCheckpoint
	err := s.store.DB().QueryRow(`SELECT generation,sequence_number FROM event_source_checkpoints WHERE mapping_uuid=? AND shard_id=?`, mappingUUID, shardID).Scan(&c.Generation, &c.SequenceNumber)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &c, nil
}
func (s *LambdaStore) SaveStreamCheckpoint(mappingUUID, shardID string, c StreamCheckpoint) error {
	result, err := s.store.DB().Exec(`INSERT INTO event_source_checkpoints(mapping_uuid,shard_id,generation,sequence_number) SELECT ?,?,?,? WHERE EXISTS(SELECT 1 FROM event_source_mappings WHERE uuid=?) ON CONFLICT(mapping_uuid,shard_id) DO UPDATE SET generation=excluded.generation,sequence_number=excluded.sequence_number`, mappingUUID, shardID, c.Generation, c.SequenceNumber, mappingUUID)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrMappingNotFound
	}
	return nil
}
