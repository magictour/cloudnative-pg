/*
Copyright The CloudNativePG Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package postgres

import (
	"fmt"
	"reflect"

	"github.com/lib/pq"
)

// SlotType represents the type of replication slot
type SlotType string

// SlotTypePhysical represents the physical replication slot
const SlotTypePhysical = "physical"

// ReplicationSlot represent the unit of a replication slot
type ReplicationSlot struct {
	Name string
	Type SlotType
}

// ReplicationSlotList contains a list of replication slot
type ReplicationSlotList struct {
	Items []ReplicationSlot
}

func (rs *ReplicationSlotList) getSlot(slotName string) *ReplicationSlot {
	for k, v := range rs.Items {
		if v.Name == slotName {
			return &rs.Items[k]
		}
	}
	return nil
}

// Has returns true if the slotName it's found in the current replication slot list
func (rs *ReplicationSlotList) Has(slotName string) bool {
	return rs.getSlot(slotName) != nil
}

// TODO compare against the active nodes in the cluster status
func (instance *Instance) getCurrentReplicationSlot() (*ReplicationSlotList, error) {
	superUserDB, err := instance.GetSuperUserDB()
	if err != nil {
		return nil, err
	}

	var replicationSlots ReplicationSlotList

	rows, err := superUserDB.Query(
		`SELECT
slot_name,
slot_type
FROM pg_replication_slots 
WHERE NOT temporary AND slot_type = 'physical'
`)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	for rows.Next() {
		var slot ReplicationSlot
		err := rows.Scan(
			&slot.Name,
			&slot.Type,
		)
		if err != nil {
			return nil, err
		}
		replicationSlots.Items = append(replicationSlots.Items, slot)
	}

	return &replicationSlots, nil
}

// UpdateReplicationsSlot will update the ReplicationSlots list in the instance list
func (instance *Instance) UpdateReplicationsSlot() error {
	if isPrimary, _ := instance.IsPrimary(); !isPrimary {
		return nil
	}
	replicationslots, err := instance.getCurrentReplicationSlot()
	if err != nil {
		return err
	}

	if !reflect.DeepEqual(instance.ReplicationSlots, replicationslots) {
		instance.ReplicationSlots = replicationslots
	}

	return nil
}

// CreateReplicationSlot will create a physical replication slot in the primary instance
func (instance *Instance) CreateReplicationSlot(slotName string) error {
	if isPrimary, _ := instance.IsPrimary(); !isPrimary {
		return nil
	}

	superUserDB, err := instance.GetSuperUserDB()
	if err != nil {
		return err
	}

	query := fmt.Sprintf(
		"SELECT * FROM pg_create_physical_replication_slot('%s')",
		pq.QuoteIdentifier(slotName))
	row := superUserDB.QueryRow(query)
	if row.Err() != nil {
		return err
	}

	instance.ReplicationSlots.Items = append(instance.ReplicationSlots.Items,
		ReplicationSlot{
			Name: slotName,
			Type: SlotTypePhysical,
		})

	return nil
}

// DeleteReplicationSlot drop the specified replication slot in the primary
func (instance *Instance) DeleteReplicationSlot(slotName string) error {
	if isPrimary, _ := instance.IsPrimary(); !isPrimary {
		return nil
	}

	superUserDB, err := instance.GetSuperUserDB()
	if err != nil {
		return err
	}

	_, err = superUserDB.Exec(fmt.Sprintf(
		"SELECT pg_drop_replication_slot('%s')",
		pq.QuoteIdentifier(slotName)))
	if err != nil {
		return err
	}

	return nil
}
