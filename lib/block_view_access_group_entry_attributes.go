package lib

import (
	"github.com/golang/glog"
	"github.com/pkg/errors"
)

// GetGroupEntryAttributeEntry returns the group entry attribute entry for the given group.
func (bav *UtxoView) GetGroupEntryAttributeEntry(groupOwnerPublicKey *PublicKey, groupKeyName *GroupKeyName,
	attributeType AccessGroupEntryAttributeType) (*AttributeEntry, error) {
	// Create accessGroupKey key.
	accessGroupKey := NewAccessGroupKey(groupOwnerPublicKey, groupKeyName[:])
	// Check if attributeType exists for the accessGroupKey. Note: If accessGroupKey does not exist in the map, attributeType won't exist either.
	if attributeEntry, exists := bav.GroupEntryAttributes[*accessGroupKey][attributeType]; exists {
		// AttributeEntry for this mapping holds IsSet bool and Value []byte.
		return attributeEntry, nil
	}

	// If utxoView doesn't have the attribute entry, check the DB.
	attributeEntry, err := DBGetAttributeEntryInGroupEntryAttributesIndex(bav.Handle, bav.Snapshot, groupOwnerPublicKey, groupKeyName, attributeType)
	if err != nil {
		return nil, errors.Wrapf(err, "GetGroupEntryAttributeEntry: Problem fetching AttributeEntry from db: ")
	}
	return attributeEntry, nil
}

// _setGroupEntryAttributeMapping sets the attribute status of a group.
func (bav *UtxoView) _setGroupEntryAttributeMapping(accessGroupKey *AccessGroupKey,
	attributeType AccessGroupEntryAttributeType, attributeEntry *AttributeEntry) error {
	// Create mapping if it doesn't exist.
	if _, exists := bav.GroupEntryAttributes[*accessGroupKey]; !exists {
		bav.GroupEntryAttributes[*accessGroupKey] = make(map[AccessGroupEntryAttributeType]*AttributeEntry)
	}
	// Set attribute.
	bav.GroupEntryAttributes[*accessGroupKey][attributeType] = attributeEntry
	return nil
}

// _deleteGroupEntryAttributeMapping deletes the entry from the GroupEntryAttributes mapping to undo any changes to
// attribute status in the current block.
func (bav *UtxoView) _deleteGroupEntryAttributeMapping(accessGroupKey *AccessGroupKey,
	attributeType AccessGroupEntryAttributeType, attributeEntry *AttributeEntry) error {

	// This function shouldn't be called with nil pointers.
	if attributeEntry == nil {
		glog.Errorf("_deleteGroupEntryAttributeMapping: Called with nil pointer")
		return nil
	}

	// Create tombstone entry and set isDeleted to true.
	tombstoneEntry := *attributeEntry
	tombstoneEntry.isDeleted = true

	// Set attribute.
	return bav._setGroupEntryAttributeMapping(accessGroupKey, attributeType, &tombstoneEntry)
}
