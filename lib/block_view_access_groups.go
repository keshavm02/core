package lib

import (
	"bytes"
	"fmt"
	"github.com/btcsuite/btcd/btcec"
	"github.com/golang/glog"
	"github.com/pkg/errors"
	"math"
	"reflect"
)

// GetMessagingMember will check the membership index for membership of memberPublicKey in the group
// <groupOwnerPublicKey, groupKeyName>. Based on the blockheight, we fetch the full group or we fetch
// the simplified message group entry from the membership index. forceFullEntry is an optional parameter that
// will force us to always fetch the full group entry.
func (bav *UtxoView) GetMessagingMember(memberPublicKey *PublicKey, groupOwnerPublicKey *PublicKey,
	groupKeyName *GroupKeyName, blockHeight uint32) *AccessGroupMember {

	// If either of the provided parameters is nil, we return.
	if memberPublicKey == nil || groupOwnerPublicKey == nil || groupKeyName == nil {
		return nil
	}

	groupMembershipKey := NewGroupMembershipKey(memberPublicKey, groupOwnerPublicKey, groupKeyName[:])

	// If the group has already been fetched in this utxoView, then we get it directly from there.
	if mapValue, exists := bav.GroupMembershipKeyToAccessGroupMember[*groupMembershipKey]; exists {
		return mapValue
	}

	// If we get here, it means that the group has not been fetched in this utxoView. We fetch it from the db.
	messagingGroupMember := DBGetMemberFromMembershipIndex(bav.Handle, bav.Snapshot, memberPublicKey, groupOwnerPublicKey, groupKeyName)
	return messagingGroupMember
}

// SetMessagingMember will set the membership index and enumeration index of GroupMessagingMember.
func (bav *UtxoView) SetMessagingMember(messagingGroupEntry *AccessGroupEntry,
	messagingGroupMember *AccessGroupMember, blockHeight uint32) {

	// set utxoView mappings
	bav._setGroupMembershipKeyToMessagingGroupMemberMapping(messagingGroupEntry, messagingGroupMember)

	//err := DBPutMessagingGroupMemberInMembershipIndex(bav.Handle, bav.Snapshot, uint64(blockHeight), messagingGroupMember, messagingGroupEntry)
	//if err != nil {
	//	return errors.Wrapf(err, "SetMessagingMember: Problem putting messaging group member in membership index: ")
	//}
	//
	//err = DBPutMessagingGroupMemberInEnumerationIndex(bav.Handle, bav.Snapshot, uint64(blockHeight), messagingGroupMember, messagingGroupEntry)
	//if err != nil {
	//	return errors.Wrapf(err, "SetMessagingMember: Problem putting messaging group member in enumeration index: ")
	//}
}

// GetMessagingEntry will check the membership index for membership of memberPublicKey in the group
// <groupOwnerPublicKey, groupKeyName>. Based on the blockheight, we fetch the full group or we fetch
// the simplified message group entry from the membership index. forceFullEntry is an optional parameter that
// will force us to always fetch the full group entry.
func (bav *UtxoView) GetMessagingEntry(memberPublicKey *PublicKey, groupOwnerPublicKey *PublicKey,
	groupKeyName *GroupKeyName, blockHeight uint32) *AccessGroupEntry {

	// If either of the provided parameters is nil, we return.
	if memberPublicKey == nil || groupOwnerPublicKey == nil || groupKeyName == nil {
		return nil
	}
	// If the group is the base key, then we return right away since base key cannot be modified by transactions.
	messagingGroupKey := NewAccessGroupKey(groupOwnerPublicKey, groupKeyName[:])
	if EqualGroupKeyName(&messagingGroupKey.GroupKeyName, BaseGroupKeyName()) {
		return &AccessGroupEntry{
			GroupOwnerPublicKey: NewPublicKey(messagingGroupKey.OwnerPublicKey[:]),
			AccessPublicKey:     NewPublicKey(messagingGroupKey.OwnerPublicKey[:]),
			AccessGroupKeyName:  BaseGroupKeyName(),
		}
	}

	// If the group has already been fetched in this utxoView, then we get it directly from there.
	if mapValue, exists := bav.AccessGroupKeyToAccessGroupEntry[*messagingGroupKey]; exists {
		return mapValue
	}

	// In case the group entry was not in utxo_view, nor was it in the membership index, we fetch the full group directly.
	return bav.GetMessagingGroupKeyToMessagingGroupEntryMapping(messagingGroupKey)
}

// GetMessagingGroupForMessagingGroupKeyExistence will check if the group with key messagingGroupKey exists, if so it will fetch
// the simplified group entry from the membership index. If the forceFullEntry is set or if we're not past the membership
// index block height, then we will fetch the entire group entry from the db (provided it exists).
func (bav *UtxoView) GetMessagingGroupForMessagingGroupKeyExistence(messagingGroupKey *AccessGroupKey,
	blockHeight uint32, forceFullEntry bool) *AccessGroupEntry {

	if messagingGroupKey == nil {
		return nil
	}

	// The owner is a member of their own group by default, hence they will be present in the membership index.
	ownerPublicKey := &messagingGroupKey.OwnerPublicKey
	groupKeyName := &messagingGroupKey.GroupKeyName
	entry := bav.GetMessagingEntry(
		ownerPublicKey, ownerPublicKey, groupKeyName, blockHeight)
	// Filter out deleted entries.
	if entry == nil || entry.isDeleted {
		return nil
	}
	return entry
}

func (bav *UtxoView) GetMessagingGroupKeyToMessagingGroupEntryMapping(
	messagingGroupKey *AccessGroupKey) *AccessGroupEntry {
	// This function is used to get a AccessGroupEntry given a AccessGroupKey. The V3 messages are
	// backwards-compatible, and in particular each user has a built-in AccessGroupKey, called the
	// "base group key," which is simply a messaging key corresponding to user's main key.
	if EqualGroupKeyName(&messagingGroupKey.GroupKeyName, BaseGroupKeyName()) {
		return &AccessGroupEntry{
			GroupOwnerPublicKey: NewPublicKey(messagingGroupKey.OwnerPublicKey[:]),
			AccessPublicKey:     NewPublicKey(messagingGroupKey.OwnerPublicKey[:]),
			AccessGroupKeyName:  BaseGroupKeyName(),
		}
	}

	// If an entry exists in the in-memory map, return the value of that mapping.
	if mapValue, exists := bav.AccessGroupKeyToAccessGroupEntry[*messagingGroupKey]; exists {
		return mapValue
	}

	// Temporarily commenting out postgres until MessagingGroup transaction are fixed.
	//if bav.Postgres != nil {
	//	var pgMessagingGroup PGMessagingGroup
	//	err := bav.Postgres.db.Model(&pgMessagingGroup).Where("group_owner_public_key = ? and messaging_group_key_name = ?",
	//		messagingGroupKey.OwnerPublicKey, messagingGroupKey.GroupKeyName).First()
	//	if err != nil {
	//		return nil
	//	}
	//
	//	memberEntries := []*AccessGroupMember{}
	//	if err := gob.NewDecoder(
	//		bytes.NewReader(pgMessagingGroup.AccessGroupMembers)).Decode(&memberEntries); err != nil {
	//		glog.Errorf("Error decoding AccessGroupMembers from DB: %v", err)
	//		return nil
	//	}
	//
	//	messagingGroupEntry := &AccessGroupEntry{
	//		GroupOwnerPublicKey:   pgMessagingGroup.GroupOwnerPublicKey,
	//		AccessPublicKey:    pgMessagingGroup.AccessPublicKey,
	//		AccessGroupKeyName: pgMessagingGroup.AccessGroupKeyName,
	//		AccessGroupMembers: memberEntries,
	//	}
	//	bav._setMessagingGroupKeyToMessagingGroupEntryMapping(&messagingGroupKey.OwnerPublicKey, messagingGroupEntry)
	//	return messagingGroupEntry
	//
	//} else {
	// If we get here it means no value exists in our in-memory map. In this case,
	// defer to the db. If a mapping exists in the db, return it. If not, return
	// nil. Either way, save the value to the in-memory UtxoView mapping.
	messagingGroupEntry := DBGetMessagingGroupEntry(bav.Handle, bav.Snapshot, messagingGroupKey)
	if messagingGroupEntry != nil {
		bav._setMessagingGroupKeyToMessagingGroupEntryMapping(&messagingGroupKey.OwnerPublicKey, messagingGroupEntry)
	}
	return messagingGroupEntry

	//}
}

func (bav *UtxoView) _setGroupMembershipKeyToMessagingGroupMemberMapping(entry *AccessGroupEntry, member *AccessGroupMember) {
	if entry == nil || member == nil {
		return
	}
	groupMembershipKey := NewGroupMembershipKey(member.GroupMemberPublicKey, entry.GroupOwnerPublicKey, entry.AccessGroupKeyName[:])
	bav.GroupMembershipKeyToAccessGroupMember[*groupMembershipKey] = member
}

func (bav *UtxoView) _setMessagingGroupKeyToMessagingGroupEntryMapping(ownerPublicKey *PublicKey,
	messagingGroupEntry *AccessGroupEntry) {

	// This function shouldn't be called with a nil entry.
	if messagingGroupEntry == nil {
		glog.Errorf("_setMessagingGroupKeyToMessagingGroupEntryMapping: Called with nil AccessGroupEntry; " +
			"this should never happen.")
		return
	}

	// Create a key for the UtxoView mapping. We always put user's owner public key as part of the map key.
	// Note that this is different from message entries, which are indexed by messaging public keys.
	messagingKey := AccessGroupKey{
		OwnerPublicKey: *ownerPublicKey,
		GroupKeyName:   *messagingGroupEntry.AccessGroupKeyName,
	}
	bav.AccessGroupKeyToAccessGroupEntry[messagingKey] = messagingGroupEntry
}

func (bav *UtxoView) _deleteMessagingGroupKeyToMessagingGroupEntryMapping(ownerPublicKey *PublicKey,
	messagingGroupEntry *AccessGroupEntry) {

	// Create a tombstone entry.
	tombstoneMessageGroupEntry := *messagingGroupEntry
	tombstoneMessageGroupEntry.isDeleted = true

	// Set the mappings to point to the tombstone entry.
	bav._setMessagingGroupKeyToMessagingGroupEntryMapping(ownerPublicKey, &tombstoneMessageGroupEntry)
}

func (bav *UtxoView) GetAccessGroupEntriesForUser(ownerPublicKey []byte, blockHeight uint32) (
	_messagingGroupEntries []*AccessGroupEntry, _err error) {
	// This function will return all groups a user is associated with,
	// including the base key group, groups the user has created, and groups where
	// the user is a recipient.

	// This is our helper map to keep track of all user messaging keys.
	messagingKeysMap := make(map[AccessGroupKey]*AccessGroupEntry)

	// Start by fetching all the messaging keys that we have in the UtxoView.
	for messagingKey, messagingKeyEntry := range bav.AccessGroupKeyToAccessGroupEntry {
		// We don't check for deleted entries now, we will do that later once we add messaging keys
		// from the DB. For now we also omit the base key, we will add it later when querying the DB.

		// Check if the messaging key corresponds to our public key.
		if reflect.DeepEqual(messagingKey.OwnerPublicKey[:], ownerPublicKey) {
			messagingKeysMap[messagingKey] = messagingKeyEntry
			continue
		}
		// Now we will look for messaging keys where the public key is a recipient of a group chat.
		if blockHeight >= bav.Params.ForkHeights.DeSoAccessGroupsBlockHeight {
			member, err := DBGetGroupMemberForMessagingGroup(bav.Handle, bav.Snapshot, messagingKeyEntry.GroupOwnerPublicKey, messagingKeyEntry.AccessGroupKeyName, NewPublicKey(ownerPublicKey))
			if err != nil {
				return nil, errors.Wrapf(err, "GetAccessGroupEntriesForUser: Problem getting group members for messaging group: %v", messagingKeyEntry)
			}
			if member != nil {
				messagingKeysMap[messagingKey] = messagingKeyEntry
			}
		} else {
			for _, recipient := range messagingKeyEntry.AccessGroupMembers {
				if reflect.DeepEqual(recipient.GroupMemberPublicKey[:], ownerPublicKey) {
					// If user is a recipient of a group chat, we need to add a modified messaging entry.
					messagingKeysMap[messagingKey] = messagingKeyEntry
					break
				}
			}
		}

	}

	// We fetched all the entries from the UtxoView, so we move to the DB.
	var dbGroupEntries []*AccessGroupEntry
	var err error
	if blockHeight >= bav.Params.ForkHeights.DeSoAccessGroupsBlockHeight {
		dbGroupEntries, err = DBGetAllUserGroupEntries(bav.Handle, bav.Snapshot, ownerPublicKey)
		if err != nil {
			return nil, errors.Wrapf(err, "GetAccessGroupEntriesForUser: problem getting "+
				"messaging group entries from the DB")
		}
	} else {
		dbGroupEntries, err = DEPRECATEDDBGetAllUserGroupEntries(bav.Handle, ownerPublicKey)
		if err != nil {
			return nil, errors.Wrapf(err, "GetAccessGroupEntriesForUser: problem getting "+
				"messaging group entries from the DB")
		}
	}
	// Now go through the messaging group entries in the DB and add keys we haven't seen before.
	for _, messagingGroupEntry := range dbGroupEntries {
		key := *NewAccessGroupKey(
			messagingGroupEntry.GroupOwnerPublicKey, messagingGroupEntry.AccessGroupKeyName[:])
		// Check if we have seen the messaging key before.
		if _, exists := messagingKeysMap[key]; !exists {
			messagingKeysMap[key] = messagingGroupEntry
		}
	}

	// We have all the user's messaging keys in our map, so we now turn them into a list.
	var retMessagingKeyEntries []*AccessGroupEntry
	for _, messagingKeyEntry := range messagingKeysMap {
		// Skip isDeleted entries
		if messagingKeyEntry.isDeleted {
			continue
		}
		retMessagingKeyEntries = append(retMessagingKeyEntries, messagingKeyEntry)
	}
	return retMessagingKeyEntries, nil
}

// TODO: Update for Postgres
func (bav *UtxoView) GetMessagesForUser(publicKey []byte, blockHeight uint32) (
	_messageEntries []*MessageEntry, _messagingKeyEntries []*AccessGroupEntry, _err error) {

	return bav.GetLimitedMessagesForUser(publicKey, math.MaxUint64, blockHeight)
}

// TODO: Update for Postgres
func (bav *UtxoView) GetLimitedMessagesForUser(ownerPublicKey []byte, limit uint64, blockHeight uint32) (
	_messageEntries []*MessageEntry, _messagingGroupEntries []*AccessGroupEntry, _err error) {

	// This function will fetch up to limit number of messages for a public key. To accomplish
	// this, we will have to fetch messages for each groups that the user has registered.

	// First get all messaging keys for a user.
	messagingGroupEntries, err := bav.GetAccessGroupEntriesForUser(ownerPublicKey, blockHeight)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "GetLimitedMessagesForUser: "+
			"problem getting user messaging keys")
	}

	// We define an auxiliary map to keep track of messages in UtxoView and DB.
	messagesMap := make(map[MessageKey]*MessageEntry)

	// First look for messages in the UtxoView. We don't skip deleted entries for now as we will do it later.
	for messageKey, messageEntry := range bav.MessageKeyToMessageEntry {
		for _, messagingKeyEntry := range messagingGroupEntries {
			if reflect.DeepEqual(messageKey.PublicKey[:], messagingKeyEntry.AccessPublicKey[:]) {
				// We will add the messages with the sender messaging public key as the MessageKey
				// so that we have no overlaps in the DB in some weird edge cases.
				mapKey := MakeMessageKey(messageEntry.SenderMessagingPublicKey[:], messageEntry.TstampNanos)
				messagesMap[mapKey] = messageEntry
				break
			}
		}
	}

	// We fetched all UtxoView entries, so now look for messages in the DB.
	dbMessageEntries, err := DBGetLimitedMessageForMessagingKeys(bav.Handle, messagingGroupEntries, limit)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "GetMessagesForUser: Problem fetching MessageEntries from db: ")
	}
	// Now iterate through all the db message entries and add them to our auxiliary map.
	for _, messageEntry := range dbMessageEntries {
		// Use the sender messaging public key for the MessageKey to make sure they match the UtxoView entries.
		mapKey := MakeMessageKey(messageEntry.SenderMessagingPublicKey[:], messageEntry.TstampNanos)
		if _, exists := messagesMap[mapKey]; !exists {
			messagesMap[mapKey] = messageEntry
		}
	}

	// We have added all message entries to our auxiliary map so now we transform them into a map.
	var messageEntries []*MessageEntry
	for _, messageEntry := range messagesMap {
		// Skip isDeleted entries
		if messageEntry.isDeleted {
			continue
		}
		messageEntries = append(messageEntries, messageEntry)
	}
	return messageEntries, messagingGroupEntries, nil
}

func ValidateGroupPublicKeyAndName(messagingPublicKey, keyName []byte) error {
	// This is a helper function that allows us to verify messaging public key and key name.

	// First validate the messagingPublicKey.
	if err := IsByteArrayValidPublicKey(messagingPublicKey); err != nil {
		return errors.Wrapf(err, "ValidateGroupPublicKeyAndName: "+
			"Problem validating sender's messaging key: %v", messagingPublicKey)
	}

	// If we get here, it means that we have a valid messaging public key.
	// Sanity-check messaging key name.
	if len(keyName) < MinMessagingKeyNameCharacters {
		return errors.Wrapf(RuleErrorMessagingKeyNameTooShort, "ValidateGroupPublicKeyAndName: "+
			"Too few characters in key name: min = %v, provided = %v",
			MinMessagingKeyNameCharacters, len(keyName))
	}
	if len(keyName) > MaxMessagingKeyNameCharacters {
		return errors.Wrapf(RuleErrorMessagingKeyNameTooLong, "ValidateGroupPublicKeyAndName: "+
			"Too many characters in key name: max = %v; provided = %v",
			MaxMessagingKeyNameCharacters, len(keyName))
	}
	return nil
}

// ValidateKeyAndNameWithUtxo validates public key and key name, which are used in DeSo V3 Messages protocol.
// The function first checks that the key and name are valid and then fetches an entry from UtxoView or DB
// to check if the key has been previously saved. This is particularly useful for connecting V3 messages.
func (bav *UtxoView) ValidateKeyAndNameWithUtxo(ownerPublicKey, messagingPublicKey, keyName []byte, blockHeight uint32) error {
	// First validate the public key and name with ValidateGroupPublicKeyAndName
	err := ValidateGroupPublicKeyAndName(messagingPublicKey, keyName)
	if err != nil {
		return errors.Wrapf(err, "ValidateKeyAndNameWithUtxo: Failed validating "+
			"messagingPublicKey and keyName")
	}

	// Fetch the messaging key entry from UtxoView.
	messagingGroupKey := NewAccessGroupKey(NewPublicKey(ownerPublicKey), keyName)
	// To validate a messaging group key, we try to fetch the simplified group entry from the membership index.
	messagingGroupEntry := bav.GetMessagingGroupForMessagingGroupKeyExistence(
		messagingGroupKey, blockHeight, false)
	if messagingGroupEntry == nil || messagingGroupEntry.isDeleted {
		return fmt.Errorf("ValidateKeyAndNameWithUtxo: non-existent messaging key entry "+
			"for ownerPublicKey: %s", PkToString(ownerPublicKey, bav.Params))
	}

	// Compare the UtxoEntry with the provided key for more validation.
	if !reflect.DeepEqual(messagingGroupEntry.AccessPublicKey[:], messagingPublicKey) {
		return fmt.Errorf("ValidateKeyAndNameWithUtxo: keys don't match for "+
			"ownerPublicKey: %s", PkToString(ownerPublicKey, bav.Params))
	}

	if !EqualGroupKeyName(messagingGroupEntry.AccessGroupKeyName, NewGroupKeyName(keyName)) {
		return fmt.Errorf("ValidateKeyAndNameWithUtxo: key name don't match for "+
			"ownerPublicKey: %s", PkToString(ownerPublicKey, bav.Params))
	}
	return nil
}

// isMemberMuted returns true if the member is muted in the group.
func (bav *UtxoView) isMemberMuted(
	messagingGroupEntry *AccessGroupEntry, memberPublicKey *PublicKey) (bool, error) {
	// Create enumeration key.
	enumerationKey := NewGroupEnumerationKey(messagingGroupEntry.GroupOwnerPublicKey, messagingGroupEntry.AccessGroupKeyName[:], memberPublicKey)
	// Check if enumerationKey exists in GroupMemberAttributes mapping.
	muted, exists := bav.GroupMemberAttributes[*enumerationKey]
	// If utxoView mapping exists, return value.
	if exists {
		return muted, nil
	}

	// If utxoView mapping doesn't exist, check DB.
	isMuted, err := DBIsMemberMutedInGroupMembersIndex(bav.Handle, bav.Snapshot, messagingGroupEntry.GroupOwnerPublicKey, messagingGroupEntry.AccessGroupKeyName, memberPublicKey)
	if err != nil {
		return false, errors.Wrapf(err, "isMemberMuted: Problem checking if member is muted in DB")
	}
	return isMuted, nil
}

// setMemberMuted sets the muted status of a member in the group.
func (bav *UtxoView) setMutedMembersMapping(
	messagingGroupEntry *AccessGroupEntry, memberPublicKey *PublicKey, muted bool) {
	// Create enumeration key.
	enumerationKey := NewGroupEnumerationKey(messagingGroupEntry.GroupOwnerPublicKey, messagingGroupEntry.AccessGroupKeyName[:], memberPublicKey)
	// Set mapping in utxoView.
	bav.GroupMemberAttributes[*enumerationKey] = muted
}

// deleteMutedMembersMapping deletes the entry from the GroupMemberAttributes mapping to reflect no change in the muted status.
func (bav *UtxoView) deleteMutedMembersMapping(
	messagingGroupEntry *AccessGroupEntry, memberPublicKey *PublicKey) {
	// Create enumeration key.
	enumerationKey := NewGroupEnumerationKey(messagingGroupEntry.GroupOwnerPublicKey, messagingGroupEntry.AccessGroupKeyName[:], memberPublicKey)
	// Delete the muted status of the member.
	delete(bav.GroupMemberAttributes, *enumerationKey)
}

// getMessagingGroupRotatingVersion returns the version of the messaging group key.
// The version is used to determine the key rotation period.
func getMessagingGroupRotatingVersion(messagingGroupEntry *AccessGroupEntry, blockHeight uint32) uint64 {
	rotatingVersion := uint64(0)
	if MigrationTriggered(uint64(blockHeight), DeSoAccessGroupsMigration) {
		// Extract ExtraData["MessageRotatingVersion"] from the entry.
		// if it's not present, leave it at 0.
		if val, exists := messagingGroupEntry.ExtraData[MessageRotatingVersion]; exists {
			// convert the []byte value to uint64
			rotatingVersion = DecodeUint64(val)
		}
	}
	return rotatingVersion
}

// setMessagingGroupRotatingVersion sets the version of the messaging group key.
// The version is used to determine the key rotation period.
func setMessagingGroupRotatingVersion(messagingGroupEntry *AccessGroupEntry, blockHeight uint32, rotatingVersion uint64) {
	if MigrationTriggered(uint64(blockHeight), DeSoAccessGroupsMigration) {
		// Set the ExtraData["MessageRotatingVersion"] to the provided value.
		messagingGroupEntry.ExtraData[MessageRotatingVersion] = EncodeUint64(rotatingVersion)
	}
}

// getMessageEntryRotatingVersion returns the version of the messaging group key.
// The version is used to determine the key rotation period.
func getMessageEntryRotatingVersion(messageEntry *MessageEntry, blockHeight uint32) uint64 {
	rotatingVersion := uint64(0)
	if MigrationTriggered(uint64(blockHeight), DeSoAccessGroupsMigration) {
		// Extract ExtraData["MessageEntryRotatingVersion"] from the entry.
		// if it's not present, leave it at 0.
		if val, exists := messageEntry.ExtraData[MessageRotatingVersion]; exists {
			// convert the []byte value to uint64
			rotatingVersion = DecodeUint64(val)
		}
	}
	return rotatingVersion
}

// setMessageEntryRotatingVersion sets the version of the messaging group key.
// The version is used to determine the key rotation period.
func setMessageEntryRotatingVersion(messageEntry *MessageEntry, blockHeight uint32, rotatingVersion uint64) {
	if MigrationTriggered(uint64(blockHeight), DeSoAccessGroupsMigration) {
		// Set the ExtraData["MessageEntryRotatingVersion"] to the provided value.
		messageEntry.ExtraData[MessageRotatingVersion] = EncodeUint64(rotatingVersion)
	}
}

func (bav *UtxoView) _connectMessagingGroup(
	txn *MsgDeSoTxn, txHash *BlockHash, blockHeight uint32, verifySignatures bool) (
	_totalInput uint64, _totalOutput uint64, _utxoOps []*UtxoOperation, _err error) {

	// Messaging groups are a part of DeSo V3 Messages.
	//
	// A AccessGroupKey is a pair of an <ownerPublicKey, groupKeyName>. MessagingGroupKeys are registered on-chain
	// and are intended to be used as senders/recipients of privateMessage transactions, as opposed to users' main
	// keys. MessagingGroupKeys solve the problem with messages for holders of derived keys, who previously had no
	// way to properly encrypt/decrypt messages, as they don't have access to user's main private key.
	//
	// A groupKeyName is a byte array between 1-32 bytes that labels the AccessGroupKey. Applications have the
	// choice to label users' MessagingGroupKeys as they desire. For instance, a groupKeyName could represent the name
	// of an on-chain group chat. On the db level, groupKeyNames are always filled to 32 bytes with []byte(0) suffix.
	//
	// We hard-code two MessagingGroupKeys:
	// 	[]byte{}              : user's ownerPublicKey. This key is registered for all users natively.
	//	[]byte("default-key") : intended to be registered when authorizing a derived key for the first time.
	//
	// The proposed flow is to register a default-key whenever first authorizing a derived key for a user. This way,
	// the derived key can be used for sending and receiving messages. DeSo V3 Messages also enable group chats, which
	// we will explain in more detail later.

	// Make sure DeSo V3 messages are live.
	if blockHeight < bav.Params.ForkHeights.DeSoV3MessagesBlockHeight {
		return 0, 0, nil, errors.Wrapf(
			RuleErrorMessagingKeyBeforeBlockHeight, "_connectMessagingGroup: "+
				"Problem connecting messaging key, too early block height")
	}

	// Check that the transaction has the right TxnType.
	if txn.TxnMeta.GetTxnType() != TxnTypeMessagingGroup {
		return 0, 0, nil, fmt.Errorf("_connectMessagingGroup: called with bad TxnType %s",
			txn.TxnMeta.GetTxnType().String())
	}
	txMeta := txn.TxnMeta.(*AccessGroupMetadata)

	// If the key name is just a list of 0s, then return because this name is reserved for the base key.
	if EqualGroupKeyName(NewGroupKeyName(txMeta.AccessGroupKeyName), BaseGroupKeyName()) {
		return 0, 0, nil, errors.Wrapf(
			RuleErrorMessagingKeyNameCannotBeZeros, "_connectMessagingGroup: "+
				"Cannot set a zeros-only key name?")
	}

	// Make sure that the messaging public key and the group key name have the correct format.
	if err := ValidateGroupPublicKeyAndName(txMeta.AccessPublicKey, txMeta.AccessGroupKeyName); err != nil {
		return 0, 0, nil, errors.Wrapf(err, "_connectMessagingGroup: "+
			"Problem parsing public key: %v", txMeta.AccessPublicKey)
	}

	// Sanity-check that transaction public key is valid.
	if err := IsByteArrayValidPublicKey(txn.PublicKey); err != nil {
		return 0, 0, nil, errors.Wrapf(err, "_connectMessagingGroup: "+
			"error %v", RuleErrorMessagingOwnerPublicKeyInvalid)
	}

	// Sanity-check that we're not trying to add a messaging public key identical to the ownerPublicKey.
	if reflect.DeepEqual(txMeta.AccessPublicKey, txn.PublicKey) {
		return 0, 0, nil, errors.Wrapf(RuleErrorMessagingPublicKeyCannotBeOwnerKey,
			"_connectMessagingGroup: messaging public key and txn public key can't be the same")
	}

	// We now have a valid messaging public key, key name, and owner public key.
	// The hard-coded default key is only intended to be registered by the owner, so we will require a signature.
	//
	// Note that we decided to relax this constraint after the fork height. Why? Because keeping it would have
	// required users to go through two confirmations when approving a key with MetaMask vs just one.
	if blockHeight < bav.Params.ForkHeights.DeSoAccessGroupsBlockHeight {
		if EqualGroupKeyName(NewGroupKeyName(txMeta.AccessGroupKeyName), DefaultGroupKeyName()) {
			// Verify the GroupOwnerSignature. it should be signature( messagingPublicKey || messagingKeyName )
			// We need to make sure the default messaging key was authorized by the master public key.
			// All other keys can be registered by derived keys.
			bytes := append(txMeta.AccessPublicKey, txMeta.AccessGroupKeyName...)
			if err := _verifyBytesSignature(txn.PublicKey, bytes, txMeta.GroupOwnerSignature, blockHeight, bav.Params); err != nil {
				return 0, 0, nil, errors.Wrapf(err, "_connectMessagingGroup: "+
					"Problem verifying signature bytes, error: %v", RuleErrorMessagingSignatureInvalid)
			}
		}
	}

	// Connect basic txn to get the total input and the total output without
	// considering the transaction metadata.
	totalInput, totalOutput, utxoOpsForTxn, err := bav._connectBasicTransfer(
		txn, txHash, blockHeight, verifySignatures)
	if err != nil {
		return 0, 0, nil, errors.Wrapf(err, "_connectMessagingGroup: ")
	}

	// We have validated all information. At this point the inputs and outputs have been processed.
	// Now we need to handle the metadata. We will proceed to add the key to UtxoView, and generate UtxoOps.

	// We support "unencrypted" groups, which are a special-case of group chats that are intended for public
	// access. For example, this could be used to make discussion groups, which anyone can discover and join.
	// To do so, we hard-code an owner public key which will index all unencrypted group chats. We choose the
	// secp256k1 base element. Essentially, unencrypted groups are treated as messaging keys that are created
	// by the base element public key. To register an unencrypted group chat, the messaging key transaction
	// should contain the base element as the messaging public key. Below, we check for this and adjust the
	// messagingGroupKey and messagingPublicKey appropriately so that we can properly index the DB entry.
	var messagingGroupKey *AccessGroupKey
	var messagingPublicKey *PublicKey
	if reflect.DeepEqual(txMeta.AccessPublicKey, GetS256BasePointCompressed()) {
		messagingGroupKey = NewAccessGroupKey(NewPublicKey(GetS256BasePointCompressed()), txMeta.AccessGroupKeyName)
		_, keyPublic := btcec.PrivKeyFromBytes(btcec.S256(), Sha256DoubleHash(txMeta.AccessGroupKeyName)[:])
		messagingPublicKey = NewPublicKey(keyPublic.SerializeCompressed())
	} else {
		messagingGroupKey = NewAccessGroupKey(NewPublicKey(txn.PublicKey), txMeta.AccessGroupKeyName)
		messagingPublicKey = NewPublicKey(txMeta.AccessPublicKey)
	}
	// First, let's check if this key doesn't already exist in UtxoView or in the DB.
	// It's worth noting that we index messaging keys by the owner public key and messaging key name.
	existingEntry := bav.GetMessagingGroupKeyToMessagingGroupEntryMapping(messagingGroupKey)

	// We will update the existing entry if it exists, or otherwise create a new utxoView entry. The new entry can currently
	// only be created if the messagingGroupOperation is AccessGroupOperationAddMembers. If we update the existing entry,
	// we will set its MessagingMembers and MuteList to these new values based on the txn.
	var newMessagingMembers []*AccessGroupMember
	var newMuteList []*AccessGroupMember
	var newUnmuteList []*AccessGroupMember

	// Determine the messaging group operation.
	var messagingGroupOperation AccessGroupOperation
	if blockHeight < bav.Params.ForkHeights.DeSoAccessGroupsBlockHeight {
		messagingGroupOperation = AccessGroupOperationAddMembers
	} else {
		messagingGroupOperation, err = GetAccessGroupOperation(txn)
		if err != nil {
			return 0, 0, nil, errors.Wrapf(err, "_connectMessagingGroup: "+
				"Problem getting messaging group operation")
		}
	}
	// Make sure that the utxoView entry and the transaction entries have the same messaging public keys and encrypted key.
	// The encrypted key is an auxiliary field that can be used to share the private key of the messaging public keys with
	// user's main key when registering a messaging key via a derived key. This field will also be used in group chats, as
	// we will later overload the AccessGroupEntry struct for storing messaging keys for group participants.
	if existingEntry != nil && !existingEntry.isDeleted {
		if !reflect.DeepEqual(existingEntry.AccessPublicKey[:], messagingPublicKey[:]) {
			return 0, 0, nil, errors.Wrapf(RuleErrorMessagingPublicKeyCannotBeDifferent,
				"_connectMessagingGroup: Messaging public key cannot differ from the existing entry")
		}
	}

	// Check what type of operation we are performing.
	switch messagingGroupOperation {
	case AccessGroupOperationAddMembers:
		// Make sure blockHeight is before the muting fork height.
		if blockHeight < bav.Params.ForkHeights.DeSoAccessGroupsBlockHeight {
			// In DeSo V3 Messages, a messaging key can initialize a group chat with more than two parties. In group chats, all
			// messages are encrypted to the group messaging public key. The group members are provided with an encrypted
			// private key of the group's messagingPublicKey so that each of them can read the messages. We refer to
			// these group members as messaging members, and for each member we will store a MessagingMember object with the
			// respective encrypted key. The encrypted key must be addressed to a registered groupKeyName for each member, e.g.
			// the base or the default key names. In particular, this design choice allows derived keys to read group messages.
			//
			// A MessagingGroup transaction can either initialize a groupMessagingKey or add more members. In the former case,
			// there will be no existing AccessGroupEntry; however, in the latter case there will be an entry present in DB
			// or UtxoView. When adding members, we need to make sure that the transaction isn't trying to change data about
			// existing members. An important limitation is that the current design doesn't support removing recipients. This
			// would be tricky to impose in consensus, considering that removed users can't *forget* the messaging private key.
			// Removing users can be facilitated in the application-layer, where we can issue a new group key and share it with
			// all valid members.

			// Map all members so that it's easier to check for overlapping members.
			existingMembers := make(map[PublicKey]bool)

			// Sanity-check a group's members can't contain the messagingPublicKey.
			existingMembers[*messagingPublicKey] = true

			// If we're adding more group members, then we need to make sure there are no overlapping members between the
			// transaction's entry, and the existing entry.
			if existingEntry != nil && !existingEntry.isDeleted {
				// We make sure we'll add at least one messaging member in the transaction.
				if len(txMeta.AccessGroupMembers) == 0 {
					return 0, 0, nil, errors.Wrapf(RuleErrorMessagingKeyDoesntAddMembers,
						"_connectMessagingGroup: Can't update a messaging key without any new recipients")
				}

				// Now iterate through all existing members and make sure there are no overlaps.
				for _, existingMember := range existingEntry.AccessGroupMembers {
					if _, exists := existingMembers[*existingMember.GroupMemberPublicKey]; exists {
						return 0, 0, nil, errors.Wrapf(
							RuleErrorMessagingMemberAlreadyExists, "_connectMessagingGroup: "+
								"Error, member already exists (%v)", existingMember.GroupMemberPublicKey)
					}

					// Add the existingMember to our helper structs.
					existingMembers[*existingMember.GroupMemberPublicKey] = true
					newMessagingMembers = append(newMessagingMembers, existingMember)
				}
			}

			// Validate all members.
			for _, messagingMember := range txMeta.AccessGroupMembers {
				// Encrypted public key cannot be empty, and has to have at least as many bytes as a generic private key.
				//
				// Note that if someone is adding themselves to an unencrypted group, then this value can be set to
				// zeros or G, the elliptic curve group element, which is also OK.
				if len(messagingMember.EncryptedKey) < btcec.PrivKeyBytesLen {
					return 0, 0, nil, errors.Wrapf(
						RuleErrorMessagingMemberEncryptedKeyTooShort, "_connectMessagingGroup: "+
							"Problem validating messagingMember encrypted key for messagingMember (%v): Encrypted "+
							"key length %v less than the minimum allowed %v. If this is an unencrypted group "+
							"member, please set %v zeros for this value", messagingMember.GroupMemberPublicKey[:],
						len(messagingMember.EncryptedKey), btcec.PrivKeyBytesLen, btcec.PrivKeyBytesLen)
				}

				// Make sure the messagingMember public key and messaging key name are valid.
				if err := ValidateGroupPublicKeyAndName(messagingMember.GroupMemberPublicKey[:], messagingMember.GroupMemberKeyName[:]); err != nil {
					return 0, 0, nil, errors.Wrapf(err, "_connectMessagingGroup: "+
						"Problem validating public key or messaging key for messagingMember (%v)", messagingMember.GroupMemberPublicKey[:])
				}

				// Now make sure messagingMember's AccessGroupKey has already been added to UtxoView or DB.
				// We encrypt the groupMessagingKey to recipients' messaging keys.
				memberMessagingGroupKey := NewAccessGroupKey(
					messagingMember.GroupMemberPublicKey, messagingMember.GroupMemberKeyName[:])
				memberGroupEntry := bav.GetMessagingGroupKeyToMessagingGroupEntryMapping(memberMessagingGroupKey)
				// The messaging key has to exist and cannot be deleted.
				if memberGroupEntry == nil || memberGroupEntry.isDeleted {
					return 0, 0, nil, errors.Wrapf(
						RuleErrorMessagingMemberKeyDoesntExist, "_connectMessagingGroup: "+
							"Problem verifying messaing key for messagingMember (%v)", messagingMember.GroupMemberPublicKey[:])
				}
				// The messagingMember can't be already added to the list of existing members.
				if _, exists := existingMembers[*messagingMember.GroupMemberPublicKey]; exists {
					return 0, 0, nil, errors.Wrapf(
						RuleErrorMessagingMemberAlreadyExists, "_connectMessagingGroup: "+
							"Error, messagingMember already exists (%v)", messagingMember.GroupMemberPublicKey[:])
				}
				// Add the messagingMember to our helper structs.
				existingMembers[*messagingMember.GroupMemberPublicKey] = true
				newMessagingMembers = append(newMessagingMembers, messagingMember)
			}

		} else { // blockHeight >= DeSoAccessGroupsBlockHeight
			// We use new optimized DB prefixes after this block height.

			// Validate all members.
			for _, messagingMember := range txMeta.AccessGroupMembers {
				// Encrypted public key cannot be empty, and has to have at least as many bytes as a generic private key.
				//
				// Note that if someone is adding themselves to an unencrypted group, then this value can be set to
				// zeros or G, the elliptic curve group element, which is also OK.
				if len(messagingMember.EncryptedKey) < btcec.PrivKeyBytesLen {
					return 0, 0, nil, errors.Wrapf(
						RuleErrorMessagingMemberEncryptedKeyTooShort, "_connectMessagingGroup: "+
							"Problem validating messagingMember encrypted key for messagingMember (%v): Encrypted "+
							"key length %v less than the minimum allowed %v. If this is an unencrypted group "+
							"member, please set %v zeros for this value", messagingMember.GroupMemberPublicKey[:],
						len(messagingMember.EncryptedKey), btcec.PrivKeyBytesLen, btcec.PrivKeyBytesLen)
				}

				// Make sure the messagingMember public key and messaging key name are valid.
				if err := ValidateGroupPublicKeyAndName(messagingMember.GroupMemberPublicKey[:], messagingMember.GroupMemberKeyName[:]); err != nil {
					return 0, 0, nil, errors.Wrapf(err, "_connectMessagingGroup: "+
						"Problem validating public key or messaging key for messagingMember (%v)", messagingMember.GroupMemberPublicKey[:])
				}

				// Now make sure messagingMember's AccessGroupKey has already been added to UtxoView or DB.
				// We encrypt the groupMessagingKey to recipients' messaging keys.
				memberMessagingGroupKey := NewAccessGroupKey(
					messagingMember.GroupMemberPublicKey, messagingMember.GroupMemberKeyName[:])
				memberGroupEntry := bav.GetMessagingGroupKeyToMessagingGroupEntryMapping(memberMessagingGroupKey)
				// The messaging key has to exist and cannot be deleted.
				if memberGroupEntry == nil || memberGroupEntry.isDeleted {
					return 0, 0, nil, errors.Wrapf(
						RuleErrorMessagingMemberKeyDoesntExist, "_connectMessagingGroup: "+
							"Problem verifying messaging key for messagingMember (%v)", messagingMember.GroupMemberPublicKey[:])
				}

				// Add the messagingMember to our helper structs.
				newMessagingMembers = append(newMessagingMembers, messagingMember)
			}
		}

	case AccessGroupOperationMuteMembers:
		// Muting members assumes the group was already created.
		if existingEntry == nil || existingEntry.isDeleted {
			return 0, 0, nil, errors.Wrapf(RuleErrorMessagingGroupDoesntExist,
				"_connectMessagingGroup: Can't mute members for a non-existent group")
		}
		// MUTING/UNMUTING functionality notes:
		// In DeSo V3 Messages, Group Chat Owners can now mute or unmute members. This essentially acts like a
		// "remove member from group" functionality, but can also be used to mute spammers in large channels.
		// Note: A muted member can still cryptographically read the past AND future messages in the group, however,
		// they cannot send messages to this group until they are unmuted by the group owner.
		// Optimization Problem and Solution:
		// Every time a new message arrives as a txn, we need to check inside _connectPrivateMessage() if the sender of
		// the message is muted or not. This would decide whether we reject a message txn or not. However, to check
		// that, we can't just fetch the entire AccessGroupEntry which may contains 1000s if not 100,000s of members.
		// Instead, we will make usage of the membership index. We will especially see this in the flushing logic.

		for _, newlyMutedMember := range txMeta.AccessGroupMembers {
			// Make sure the IsMuted field is currently set to false as the member is not muted yet.
			if newlyMutedMember.IsMuted {
				return 0, 0, nil, errors.Wrapf(
					RuleErrorMessagingMemberIsMutedAlreadySetToTrue, "_connectMessagingGroup: "+
						"Problem validating messagingMember IsMuted field for messagingMember (%v): IsMuted field "+
						"is already set to true", newlyMutedMember.GroupMemberPublicKey[:])
			}
			// Make sure GroupOwner is not muting herself
			if reflect.DeepEqual(newlyMutedMember.GroupMemberPublicKey[:], existingEntry.GroupOwnerPublicKey[:]) {
				return 0, 0, nil, errors.Wrapf(RuleErrorMessagingGroupOwnerMutingSelf,
					"_connectMessagingGroup: GroupOwner cannot mute herself (%v).", existingEntry.GroupOwnerPublicKey[:])
			}
			// Make sure we are muting a member that exists in the group.
			member := bav.GetMessagingMember(newlyMutedMember.GroupMemberPublicKey, existingEntry.GroupOwnerPublicKey, existingEntry.AccessGroupKeyName, blockHeight)
			if member == nil {
				return 0, 0, nil, errors.Wrapf(RuleErrorMessagingMemberNotInGroup,
					"_connectMessagingGroup: Can't mute a non-existent member (%v)", newlyMutedMember.GroupMemberPublicKey[:])
			}

			// Set IsMuted to true.
			newlyMutedMember.IsMuted = true
			// Add member to newMuteList
			newMuteList = append(newMuteList, newlyMutedMember)
		}

	case AccessGroupOperationUnmuteMembers:
		// Unmuting members assumes the group was already created.
		if existingEntry == nil || existingEntry.isDeleted {
			return 0, 0, nil, errors.Wrapf(RuleErrorMessagingGroupDoesntExist,
				"_connectMessagingGroup: Can't mute members for a non-existent group")
		}

		for _, newlyUnmutedMember := range txMeta.AccessGroupMembers {

			// Make sure we are unmuting a member that exists in the group.
			member := bav.GetMessagingMember(newlyUnmutedMember.GroupMemberPublicKey, existingEntry.GroupOwnerPublicKey, existingEntry.AccessGroupKeyName, blockHeight)
			if member == nil {
				return 0, 0, nil, errors.Wrapf(RuleErrorMessagingMemberNotInGroup,
					"_connectMessagingGroup: Can't unmute a non-existent member (%v)", newlyUnmutedMember.GroupMemberPublicKey[:])
			}

			// GroupOwner unmuting herself is invalid because GroupOwner can never be muted in the first place
			if reflect.DeepEqual(newlyUnmutedMember.GroupMemberPublicKey[:], existingEntry.GroupOwnerPublicKey[:]) {
				return 0, 0, nil, errors.Wrapf(RuleErrorMessagingGroupOwnerUnmutingSelf,
					"_connectMessagingGroup: GroupOwner cannot mute herself (%v).", existingEntry.GroupOwnerPublicKey[:])
			}

			// Set IsMuted to false.
			newlyUnmutedMember.IsMuted = false
			// Add member to newUnmuteList
			newUnmuteList = append(newUnmuteList, newlyUnmutedMember)
		}

	default:
		// If we're here, then the operation type is invalid. Currently, this can only
		// happen if the operation is of type AccessGroupOperationRemoveMembers
		return 0, 0, nil, errors.Wrapf(err,
			"_connectMessagingGroup: Error, messaging group operation.")

	}

	// merge extra data
	var extraData map[string][]byte
	if blockHeight >= bav.Params.ForkHeights.ExtraDataOnEntriesBlockHeight {
		var existingExtraData map[string][]byte
		if existingEntry != nil && !existingEntry.isDeleted {
			existingExtraData = existingEntry.ExtraData
		}
		extraData = mergeExtraData(existingExtraData, txn.ExtraData)
	}

	// TODO: Currently, it is technically possible for any user to add *any other* user to *any group* with
	// a garbage EncryptedKey. This can be filtered out at the app layer, though, and for now it leaves the
	// app layer with more flexibility compared to if we implemented an explicit permissioning model at the
	// consensus level.

	// Create a AccessGroupEntry so we can add the entry to UtxoView.
	messagingGroupEntry := NewAccessGroupEntry(
		&messagingGroupKey.OwnerPublicKey,
		messagingPublicKey,
		NewGroupKeyName(txMeta.AccessGroupKeyName),
		newMessagingMembers,
		extraData,
		uint64(blockHeight),
	)

	// Create a utxoOps entry, we make a copy of the existing entry.
	var prevMessagingGroupEntry *AccessGroupEntry
	if existingEntry != nil && !existingEntry.isDeleted {
		prevMessagingGroupEntry = &AccessGroupEntry{}
		rr := bytes.NewReader(EncodeToBytes(uint64(blockHeight), existingEntry))
		if exists, err := DecodeFromBytes(prevMessagingGroupEntry, rr); !exists || err != nil {
			return 0, 0, nil, errors.Wrapf(err,
				"_connectMessagingGroup: Error decoding previous entry")
		}
	}

	// Set mappings and DB entries if blockHeight is greater than muting fork height.
	if blockHeight >= bav.Params.ForkHeights.DeSoAccessGroupsBlockHeight {
		// Set mappings for newlyMutedMembers
		for _, newlyMutedMember := range newMuteList {
			bav.SetMessagingMember(messagingGroupEntry, newlyMutedMember, blockHeight)
		}
		// Set mappings for newlyUnmutedMembers
		for _, newlyUnmutedMember := range newUnmuteList {
			bav.SetMessagingMember(messagingGroupEntry, newlyUnmutedMember, blockHeight)
		}
	}

	bav._setMessagingGroupKeyToMessagingGroupEntryMapping(&messagingGroupKey.OwnerPublicKey, messagingGroupEntry)

	// Construct UtxoOperation.
	utxoOpsForTxn = append(utxoOpsForTxn, &UtxoOperation{
		Type:                  OperationTypeMessagingKey,
		PrevMessagingKeyEntry: prevMessagingGroupEntry,
	})

	return totalInput, totalOutput, utxoOpsForTxn, nil
}

func (bav *UtxoView) _disconnectMessagingGroup(
	operationType OperationType, currentTxn *MsgDeSoTxn, txnHash *BlockHash,
	utxoOpsForTxn []*UtxoOperation, blockHeight uint32) error {

	// Verify that the last operation is a AccessGroupKey operation
	if len(utxoOpsForTxn) == 0 {
		return fmt.Errorf("_disconnectMessagingGroup: utxoOperations are missing")
	}
	operationIndex := len(utxoOpsForTxn) - 1
	if utxoOpsForTxn[operationIndex].Type != OperationTypeMessagingKey {
		return fmt.Errorf("_disconnectMessagingGroup: Trying to revert "+
			"OperationTypeMessagingKey but found type %v",
			utxoOpsForTxn[operationIndex].Type)
	}

	// Check that the transaction has the right TxnType.
	if currentTxn.TxnMeta.GetTxnType() != TxnTypeMessagingGroup {
		return fmt.Errorf("_disconnectMessagingGroup: called with bad TxnType %s",
			currentTxn.TxnMeta.GetTxnType().String())
	}

	// Now we know the txMeta is AccessGroupKey
	txMeta := currentTxn.TxnMeta.(*AccessGroupMetadata)

	// Sanity check that the messaging public key and key name are valid
	err := ValidateGroupPublicKeyAndName(txMeta.AccessPublicKey, txMeta.AccessGroupKeyName)
	if err != nil {
		return errors.Wrapf(err, "_disconnectMessagingGroup: failed validating the messaging "+
			"public key and key name")
	}

	// Get the messaging key that the transaction metadata points to.
	var messagingKey *AccessGroupKey
	if reflect.DeepEqual(txMeta.AccessPublicKey, GetS256BasePointCompressed()) {
		messagingKey = NewAccessGroupKey(NewPublicKey(GetS256BasePointCompressed()), txMeta.AccessGroupKeyName)
	} else {
		messagingKey = NewAccessGroupKey(NewPublicKey(currentTxn.PublicKey), txMeta.AccessGroupKeyName)
	}

	messagingKeyEntry := bav.GetMessagingGroupKeyToMessagingGroupEntryMapping(messagingKey)
	if messagingKeyEntry == nil || messagingKeyEntry.isDeleted {
		return fmt.Errorf("_disconnectBasicTransfer: Error, this key was already deleted "+
			"messagingKey: %v", messagingKey)
	}
	prevMessagingKeyEntry := utxoOpsForTxn[operationIndex].PrevMessagingKeyEntry
	// sanity check that the prev entry and current entry match
	if prevMessagingKeyEntry != nil {
		if !reflect.DeepEqual(messagingKeyEntry.AccessPublicKey[:], prevMessagingKeyEntry.AccessPublicKey[:]) ||
			!reflect.DeepEqual(messagingKeyEntry.GroupOwnerPublicKey[:], prevMessagingKeyEntry.GroupOwnerPublicKey[:]) ||
			!EqualGroupKeyName(messagingKeyEntry.AccessGroupKeyName, prevMessagingKeyEntry.AccessGroupKeyName) {

			return fmt.Errorf("_disconnectBasicTransfer: Error, this key was already deleted "+
				"messagingKey: %v", messagingKey)
		}
	}

	// Delete this item from UtxoView to indicate we should remove this entry from DB.
	bav._deleteMessagingGroupKeyToMessagingGroupEntryMapping(&messagingKey.OwnerPublicKey, messagingKeyEntry)
	// If the previous entry exists, we should set it in the utxoview
	if prevMessagingKeyEntry != nil {
		bav._setMessagingGroupKeyToMessagingGroupEntryMapping(&messagingKey.OwnerPublicKey, prevMessagingKeyEntry)
	}

	// Now disconnect the basic transfer.
	return bav._disconnectBasicTransfer(
		currentTxn, txnHash, utxoOpsForTxn[:operationIndex], blockHeight)
}