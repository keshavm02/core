package lib

import (
	"github.com/golang/glog"
	"github.com/pkg/errors"
)

// GetGroupMemberAttributeEntry returns the group member attribute entry for the given group member.
func (bav *UtxoView) GetGroupMemberAttributeEntry(enumerationKey *GroupEnumerationKey,
	attributeType AccessGroupMemberAttributeType) (*AttributeEntry, error) {
	// Check if attributeType exists for the enumerationKey. Note: If enumerationKey does not exist in the map, attributeType won't exist either.
	if attributeEntry, exists := bav.GroupMemberAttributes[*enumerationKey][attributeType]; exists {
		// AttributeEntry for this mapping holds IsSet bool and Value []byte.
		return attributeEntry, nil
	}

	// If utxoView doesn't have the attribute entry, check the DB.
	attributeEntry, err := DBGetAttributeEntryInGroupMemberAttributesIndex(bav.Handle, bav.Snapshot,
		&enumerationKey.GroupOwnerPublicKey, &enumerationKey.GroupKeyName, &enumerationKey.GroupMemberPublicKey, attributeType)
	if err != nil {
		return nil, errors.Wrapf(err, "GetGroupMemberAttributeEntry: Problem fetching AttributeEntry from db: ")
	}
	return attributeEntry, nil
}

// _setGroupMemberAttributeMapping sets the muted status of a member in the group.
func (bav *UtxoView) _setGroupMemberAttributeMapping(enumerationKey *GroupEnumerationKey,
	attributeType AccessGroupMemberAttributeType, attributeEntry *AttributeEntry) error {
	// Create mapping if it doesn't exist.
	if _, exists := bav.GroupMemberAttributes[*enumerationKey]; !exists {
		bav.GroupMemberAttributes[*enumerationKey] = make(map[AccessGroupMemberAttributeType]*AttributeEntry)
	}
	// Set attribute.
	bav.GroupMemberAttributes[*enumerationKey][attributeType] = attributeEntry
	return nil
}

// _deleteGroupMemberAttributeMapping deletes the entry from the GroupMemberAttributes mapping to undo any changes to
// attribute status in the current block.
func (bav *UtxoView) _deleteGroupMemberAttributeMapping(enumerationKey *GroupEnumerationKey,
	attributeType AccessGroupMemberAttributeType, attributeEntry *AttributeEntry) error {

	// This function shouldn't be called with nil pointers.
	if enumerationKey == nil || attributeEntry == nil {
		glog.Errorf("_deleteGroupMemberAttributeMapping: Called with nil pointer")
		return nil
	}

	// Create tombstone entry and set isDeleted to true.
	tombstoneEntry := *attributeEntry
	tombstoneEntry.isDeleted = true

	// Set attribute.
	return bav._setGroupMemberAttributeMapping(enumerationKey, attributeType, &tombstoneEntry)
}
