#include <assert.h>
#include <string.h>
#include "btree.h"

KeyValuePair::KeyValuePair() { }


KeyValuePair::KeyValuePair(const KEY_T &k, const VALUE_T &v) : key(k), value(v) { }


KeyValuePair::KeyValuePair(const KeyValuePair &rhs) : key(rhs.key), value(rhs.value) { }


KeyValuePair::~KeyValuePair() { }


KeyValuePair &KeyValuePair::operator=(const KeyValuePair &rhs) {
    return *(new(this) KeyValuePair(rhs));
}

BTreeIndex::BTreeIndex(SIZE_T keysize, SIZE_T valuesize, BufferCache *cache, bool unique) {
    superblock.info.keysize = keysize;
    superblock.info.valuesize = valuesize;
    buffercache = cache;
    // note: ignoring unique now
}

BTreeIndex::BTreeIndex() {
    // shouldn't have to do anything
}


//
// Note, will not attach!
//
BTreeIndex::BTreeIndex(const BTreeIndex &rhs) {
    buffercache = rhs.buffercache;
    superblock_index = rhs.superblock_index;
    superblock = rhs.superblock;
}

BTreeIndex::~BTreeIndex() {
    // shouldn't have to do anything
}


BTreeIndex &BTreeIndex::operator=(const BTreeIndex &rhs) {
    return *(new(this)BTreeIndex(rhs));
}


ERROR_T BTreeIndex::AllocateNode(SIZE_T &n) {
    n = superblock.info.freelist;

    if (n == 0) {
        return ERROR_NOSPACE;
    }

    BTreeNode node;

    node.Unserialize(buffercache, n);
    assert(node.info.nodetype == BTREE_UNALLOCATED_BLOCK);
    superblock.info.freelist = node.info.freelist;
    superblock.Serialize(buffercache, superblock_index);
    buffercache->NotifyAllocateBlock(n);

    return ERROR_NOERROR;
}


ERROR_T BTreeIndex::DeallocateNode(const SIZE_T &n) {
    BTreeNode node;

    node.Unserialize(buffercache, n);
    assert(node.info.nodetype != BTREE_UNALLOCATED_BLOCK);
    node.info.nodetype = BTREE_UNALLOCATED_BLOCK;
    node.info.freelist = superblock.info.freelist;
    node.Serialize(buffercache, n);
    superblock.info.freelist = n;
    superblock.Serialize(buffercache, superblock_index);
    buffercache->NotifyDeallocateBlock(n);

    return ERROR_NOERROR;

}

ERROR_T BTreeIndex::Attach(const SIZE_T initblock, const bool create) {
    ERROR_T errorMessage;

    superblock_index = initblock;
    assert(superblock_index == 0);

    if (create) {
        // build a super block, root node, and a free space list
        //
        // Superblock at superblock_index
        // root node at superblock_index+1
        // free space list for rest
        BTreeNode newsuperblock(BTREE_SUPERBLOCK, superblock.info.keysize, superblock.info.valuesize,
                                buffercache->GetBlockSize());
        newsuperblock.info.rootnode = superblock_index + 1;
        newsuperblock.info.freelist = superblock_index + 2;
        newsuperblock.info.numkeys = 0;

        buffercache->NotifyAllocateBlock(superblock_index);

        errorMessage = newsuperblock.Serialize(buffercache, superblock_index);

        if (errorMessage) {
            return errorMessage;
        }

        BTreeNode newrootnode(BTREE_ROOT_NODE, superblock.info.keysize, superblock.info.valuesize,
                              buffercache->GetBlockSize());
        newrootnode.info.rootnode = superblock_index + 1;
        newrootnode.info.freelist = superblock_index + 2;
        newrootnode.info.numkeys = 0;

        buffercache->NotifyAllocateBlock(superblock_index + 1);

        errorMessage = newrootnode.Serialize(buffercache, superblock_index + 1);

        if (errorMessage) {
            return errorMessage;
        }

        for (SIZE_T i = superblock_index + 2; i < buffercache->GetNumBlocks(); i++) {
            BTreeNode newfreenode(BTREE_UNALLOCATED_BLOCK, superblock.info.keysize, superblock.info.valuesize,
                                  buffercache->GetBlockSize());
            newfreenode.info.rootnode = superblock_index + 1;
            newfreenode.info.freelist = ((i + 1) == buffercache->GetNumBlocks()) ? 0 : i + 1;

            errorMessage = newfreenode.Serialize(buffercache, i);

            if (errorMessage) {
                return errorMessage;
            }

        }
    }

    // OK, now, mounting the btree is simply a matter of reading the superblock

    return superblock.Unserialize(buffercache, initblock);
}


ERROR_T BTreeIndex::Detach(SIZE_T &initblock) {
    return superblock.Serialize(buffercache, superblock_index);
}


ERROR_T BTreeIndex::LookupOrUpdateInternal(const SIZE_T &node, const BTreeOp op, const KEY_T &key, VALUE_T &value) {
    BTreeNode dummy;
    ERROR_T errorMessage;
    SIZE_T position;
    KEY_T testkey;
    SIZE_T ptr;

    errorMessage = dummy.Unserialize(buffercache, node);

    if (errorMessage != ERROR_NOERROR) {
        return errorMessage;
    }

    switch (dummy.info.nodetype) {
        case BTREE_ROOT_NODE:
        case BTREE_INTERIOR_NODE:
            // Scan through key/ptr pairs
            //and recurse if possible
            for (position = 0; position < dummy.info.numkeys; position++) {
                errorMessage = dummy.GetKey(position, testkey);
                if (errorMessage) { return errorMessage; }
                if (key < testkey || key == testkey) {
                    // OK, so we now have the first key that's larger
                    // so we ned to recurse on the ptr immediately previous to
                    // this one, if it exists
                    errorMessage = dummy.GetPtr(position, ptr);
                    if (errorMessage) { return errorMessage; }
                    return LookupOrUpdateInternal(ptr, op, key, value);
                }
            }
            // if we got here, we need to go to the next pointer, if it exists
            if (dummy.info.numkeys > 0) {
                errorMessage = dummy.GetPtr(dummy.info.numkeys, ptr);
                if (errorMessage) { return errorMessage; }
                return LookupOrUpdateInternal(ptr, op, key, value);
            } else {
                // There are no keys at all on this node, so nowhere to go
                return ERROR_NONEXISTENT;
            }
            break;
        case BTREE_LEAF_NODE:
            // Scan through keys looking for matching value
            for (position = 0; position < dummy.info.numkeys; position++) {
                errorMessage = dummy.GetKey(position, testkey);
                if (errorMessage) { return errorMessage; }
                if (testkey == key) {
                    if (op == BTREE_OP_LOOKUP) {
                        return dummy.GetVal(position, value);
                    } else {
                        if ((errorMessage = dummy.SetVal(position, value))) return errorMessage;
                        return dummy.Serialize(buffercache, node);
                    }
                }
            }
            return ERROR_NONEXISTENT;
            break;
        default:
            // We can't be looking at anything other than a root, internal, or leaf
            return ERROR_INSANE;
            break;
    }

    return ERROR_INSANE;
}


static ERROR_T PrintNode(ostream &os, SIZE_T nodenum, BTreeNode &dummy, BTreeDisplayType dt) {
    KEY_T key;
    VALUE_T value;
    SIZE_T ptr;
    SIZE_T position;
    ERROR_T errorMessage;
    unsigned i;

    if (dt == BTREE_DEPTH_DOT) {
        os << nodenum << " [ label=\"" << nodenum << ": ";
    } else if (dt == BTREE_DEPTH) {
        os << nodenum << ": ";
    } else {
    }

    switch (dummy.info.nodetype) {
        case BTREE_ROOT_NODE:
        case BTREE_INTERIOR_NODE:
            if (dt == BTREE_SORTED_KEYVAL) {
            } else {
                if (dt == BTREE_DEPTH_DOT) {
                } else {
                    os << "Interior: ";
                }
                for (position = 0; position <= dummy.info.numkeys; position++) {
                    errorMessage = dummy.GetPtr(position, ptr);
                    if (errorMessage) { return errorMessage; }
                    os << "*" << ptr << " ";
                    // Last pointer
                    if (position == dummy.info.numkeys) break;
                    errorMessage = dummy.GetKey(position, key);
                    if (errorMessage) { return errorMessage; }
                    for (i = 0; i < dummy.info.keysize; i++) {
                        os << key.data[i];
                    }
                    os << " ";
                }
            }
            break;
        case BTREE_LEAF_NODE:
            if (dt == BTREE_DEPTH_DOT || dt == BTREE_SORTED_KEYVAL) {
            } else {
                os << "Leaf: ";
            }
            for (position = 0; position < dummy.info.numkeys; position++) {
                if (position == 0) {
                    // special case for first pointer
                    errorMessage = dummy.GetPtr(position, ptr);
                    if (errorMessage) { return errorMessage; }
                    if (dt != BTREE_SORTED_KEYVAL) {
                        os << "*" << ptr << " ";
                    }
                }
                if (dt == BTREE_SORTED_KEYVAL) {
                    os << "(";
                }
                errorMessage = dummy.GetKey(position, key);
                if (errorMessage) { return errorMessage; }
                for (i = 0; i < dummy.info.keysize; i++) {
                    os << key.data[i];
                }
                if (dt == BTREE_SORTED_KEYVAL) {
                    os << ",";
                } else {
                    os << " ";
                }
                errorMessage = dummy.GetVal(position, value);
                if (errorMessage) { return errorMessage; }
                for (i = 0; i < dummy.info.valuesize; i++) {
                    os << value.data[i];
                }
                if (dt == BTREE_SORTED_KEYVAL) {
                    os << ")\n";
                } else {
                    os << " ";
                }
            }
            break;
        default:
            if (dt == BTREE_DEPTH_DOT) {
                os << "Unknown(" << dummy.info.nodetype << ")";
            } else {
                os << "Unsupported Node Type " << dummy.info.nodetype;
            }
    }
    if (dt == BTREE_DEPTH_DOT) {
        os << "\" ]";
    }
    return ERROR_NOERROR;
}


ERROR_T BTreeIndex::Lookup(const KEY_T &key, VALUE_T &value) {
    return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_LOOKUP, key, value);
}



ERROR_T BTreeIndex::SplitNode(const SIZE_T &node, SIZE_T &newNode, KEY_T &middle) {
    BTreeNode leftNode, rightNode, leafnode;
    SIZE_T leftKeyNum, rightKeyNum;
    char *src, *dest;
    ERROR_T errorMessage;

    leftNode.Unserialize(buffercache, node);
    rightNode = leftNode;
    if ((errorMessage = AllocateNode(newNode))) return errorMessage;
    leftKeyNum = leftNode.info.numkeys / 2 + 1;               //left = right + 1
    rightKeyNum = leftNode.info.numkeys - leftKeyNum;
    if (leftNode.info.nodetype == BTREE_LEAF_NODE) {
        
        leftNode.GetKey(leftKeyNum - 1, middle);                 // give split a value
        src = leftNode.ResolveKeyVal(leftKeyNum);                  //last key in leftnode
        dest = rightNode.ResolveKeyVal(0);                          //first key in rightnode
        memmove(dest, src, rightKeyNum * (leftNode.info.keysize + leftNode.info.valuesize));
    } else {
        leftKeyNum = leftKeyNum -1;
        rightKeyNum = rightKeyNum- 1;
        leftNode.GetKey(leftKeyNum, middle);
        src = leftNode.ResolvePtr(leftKeyNum + 1);
        dest = rightNode.ResolvePtr(0);
        memmove(dest, src, rightKeyNum * (leftNode.info.keysize + sizeof(SIZE_T)) + sizeof(SIZE_T));


    }
    leftNode.info.numkeys = leftKeyNum;
    rightNode.info.numkeys = rightKeyNum;

    if ((errorMessage = leftNode.Serialize(buffercache, node))) return errorMessage;
    if ((errorMessage = rightNode.Serialize(buffercache, newNode))) return errorMessage;
    return ERROR_NOERROR;
}

//
ERROR_T BTreeIndex::InsertOneNode(const SIZE_T node, const KEY_T &key, const VALUE_T &value, const SIZE_T &newNode) {
    BTreeNode dummy;
    KEY_T testkey;
    SIZE_T numkeys;
    SIZE_T position;
    ERROR_T errorMessage;
    KEY_T prevKey;
    VALUE_T prevVal;
    char *src,*dest;

    dummy.Unserialize(buffercache, node);
    numkeys = dummy.info.numkeys;

    dummy.info.numkeys++;

    if (dummy.info.numkeys == 1) {
        if ((errorMessage = dummy.SetKey(0, key))) return errorMessage;
        if ((errorMessage = dummy.SetVal(0, value))) return errorMessage;
    } else {
        for (position = 0; position < numkeys; position++) {
            if ((errorMessage = dummy.GetKey(position, testkey))) return errorMessage;
            if (key < testkey) {
                src = dummy.ResolveKey(position);
                dest = dummy.ResolveKey(position + 1);
                if (dummy.info.nodetype == BTREE_LEAF_NODE) {
                    for (int position2 = (int) dummy.info.numkeys - 2; position2 >= (int) position; position2--)
              {
                // grab old key and value
                dummy.GetKey((SIZE_T) position2, prevKey);
                dummy.GetVal((SIZE_T) position2, prevVal);
                dummy.SetKeyVal((SIZE_T) position2 + 1, KeyValuePair(prevKey, prevVal));

              }
                    
                  
                    if ((errorMessage = dummy.SetKey(position, key))) return errorMessage;
                    if ((errorMessage = dummy.SetVal(position, value))) return errorMessage;
                } else {
                    for (int position2 = (int) dummy.info.numkeys - 2; position2 >= (int) position; position2--)
              {
                // grab old key and value
                dummy.GetKey((SIZE_T) position2, prevKey);
                dummy.GetVal((SIZE_T) position2, prevVal);
                dummy.SetKeyVal((SIZE_T) position2 + 1, KeyValuePair(prevKey, prevVal));

              }
                    if ((errorMessage = dummy.SetKey(position, key))) return errorMessage;
                    if ((errorMessage = dummy.SetPtr(position + 1, newNode))) return errorMessage;
                }
                break;
            }
            if (position == numkeys - 1) {
                if (dummy.info.nodetype == BTREE_LEAF_NODE) {
                    if ((errorMessage = dummy.SetKey(numkeys, key))) return errorMessage;
                    if ((errorMessage = dummy.SetVal(numkeys, value))) return errorMessage;
                } else {
                    if ((errorMessage = dummy.SetKey(numkeys, key))) return errorMessage;
                    if ((errorMessage = dummy.SetPtr(numkeys + 1, newNode))) return errorMessage;
                }
                break;
            }
        }
    }
    return dummy.Serialize(buffercache, node);
}


ERROR_T BTreeIndex::splitInsert(const SIZE_T &node, const KEY_T &key, const VALUE_T &value) {

    BTreeNode dummy;
    ERROR_T errorMessage;
    SIZE_T position;
    KEY_T testkey;
    SIZE_T ptr;
    SIZE_T newNode;
    KEY_T middle;
    BTreeNode temp;

    dummy.Unserialize(buffercache, node);
    switch (dummy.info.nodetype) {
        case BTREE_ROOT_NODE:
        case BTREE_INTERIOR_NODE:
            for (position = 0; position < dummy.info.numkeys; position++) {
                if ((errorMessage = dummy.GetKey(position, testkey))) return errorMessage;
                if (key < testkey || key == testkey) {
                    if ((errorMessage = dummy.GetPtr(position, ptr))) return errorMessage;
                    if ((errorMessage = splitInsert(ptr, key, value))) return errorMessage;
                     
                    temp.Unserialize(buffercache, ptr);
                    // dummy.info.GetNumSlotsAsLeaf();
                    int interior =  temp.info.GetNumSlotsAsInterior();
                    int leaf1 = temp.info.GetNumSlotsAsLeaf();
                    // cout << "interior= " << interior<< endl;
                    // cout << "leaf1= " << leaf1<< endl;
                    if((temp.info.nodetype == BTREE_INTERIOR_NODE && temp.info.numkeys == temp.info.GetNumSlotsAsInterior()) || (temp.info.nodetype == BTREE_LEAF_NODE && temp.info.numkeys == temp.info.GetNumSlotsAsLeaf())) 
                    {
                        if ((errorMessage = SplitNode(ptr, newNode, middle))) return errorMessage;
                        return InsertOneNode(node, middle, VALUE_T(), newNode);
                    }
                    return errorMessage;
                }
            }
            if (dummy.info.numkeys > 0) {
                if ((errorMessage = dummy.GetPtr(dummy.info.numkeys, ptr))) return errorMessage;
                if ((errorMessage = splitInsert(ptr, key, value))) return errorMessage;
                 temp.Unserialize(buffercache, ptr);

                 int interior =  temp.info.GetNumSlotsAsInterior();
                    int leaf1 = temp.info.GetNumSlotsAsLeaf();
                    // cout << "interior= " << interior<< endl;
                    // cout << "leaf1= " << leaf1<< endl;
                if((temp.info.nodetype == BTREE_INTERIOR_NODE && temp.info.numkeys == temp.info.GetNumSlotsAsInterior() ) || (temp.info.nodetype == BTREE_LEAF_NODE && temp.info.numkeys == temp.info.GetNumSlotsAsLeaf())) 
                {
                  
                    if ((errorMessage = SplitNode(ptr, newNode, middle))) return errorMessage;
                   return InsertOneNode(node, middle, VALUE_T(), newNode);
                }
                return errorMessage;
            }
            return ERROR_INSANE;
        case BTREE_LEAF_NODE:
            return InsertOneNode(node, key, value, 0);
        default:
            return ERROR_INSANE;
    }
}


ERROR_T BTreeIndex::Insert(const KEY_T &key, const VALUE_T &value) {
    VALUE_T v = value;
    if (Lookup(key, v) != ERROR_NONEXISTENT) {
        return ERROR_CONFLICT;
    }

    ERROR_T errorMessage;
    BTreeNode dummy(BTREE_LEAF_NODE, superblock.info.keysize, superblock.info.valuesize, buffercache->GetBlockSize());
    BTreeNode rootNode;
    rootNode.Unserialize(buffercache, superblock.info.rootnode);
    BTreeNode temp;


    //if no node exists, create a new root node, and connect it with two leaf nodes.
    if (rootNode.info.numkeys == 0) {
        SIZE_T leftNode, rightNode;
        if ((errorMessage = AllocateNode(leftNode))) return errorMessage;
        if ((errorMessage = AllocateNode(rightNode))) return errorMessage;
        dummy.Serialize(buffercache, leftNode);
        dummy.Serialize(buffercache, rightNode);
        rootNode.info.numkeys = 1;
        rootNode.SetKey(0, key);
        rootNode.SetPtr(0, leftNode);
        rootNode.SetPtr(1, rightNode);
        rootNode.Serialize(buffercache, superblock.info.rootnode);
    }

    SIZE_T oldRoot, newNode;
    KEY_T middle;
    oldRoot = superblock.info.rootnode;
    errorMessage = splitInsert(superblock.info.rootnode,key,value);
     temp.Unserialize(buffercache, superblock.info.rootnode);

     int interior =  temp.info.GetNumSlotsAsInterior() / 66;
     int leaf1 = temp.info.GetNumSlotsAsLeaf() / 66;
    if((temp.info.nodetype == BTREE_INTERIOR_NODE && temp.info.numkeys == temp.info.GetNumSlotsAsInterior() ) || (temp.info.nodetype == BTREE_LEAF_NODE && temp.info.numkeys == temp.info.GetNumSlotsAsLeaf())) 
      { 
        SplitNode(oldRoot, newNode, middle);
        if ((errorMessage = AllocateNode(superblock.info.rootnode))) return errorMessage;
        rootNode.info.numkeys = 1;
        rootNode.SetKey(0, middle);
        rootNode.SetPtr(0, oldRoot);
        rootNode.SetPtr(1, newNode);
        return rootNode.Serialize(buffercache, superblock.info.rootnode);
       }
    return errorMessage;
}


ERROR_T BTreeIndex::Update(const KEY_T &key, const VALUE_T &value) {
    VALUE_T v(value);
    return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_UPDATE, key, v);
}


ERROR_T BTreeIndex::Delete(const KEY_T &key) {
    // This is optional extra credit
    //
    //
    
    return ERROR_UNIMPL;
}


//
//
// DEPTH first traversal
// DOT is Depth + DOT format
//

ERROR_T BTreeIndex::DisplayInternal(const SIZE_T &node, ostream &o, BTreeDisplayType display_type) const {
    KEY_T testkey;
    SIZE_T ptr;
    BTreeNode dummy;
    ERROR_T errorMessage;
    SIZE_T position;

    errorMessage = dummy.Unserialize(buffercache, node);

    if (errorMessage != ERROR_NOERROR) {
        return errorMessage;
    }

    errorMessage = PrintNode(o, node, dummy, display_type);

    if (errorMessage) { return errorMessage; }

    if (display_type == BTREE_DEPTH_DOT) {
        o << ";";
    }

    if (display_type != BTREE_SORTED_KEYVAL) {
        o << endl;
    }

    switch (dummy.info.nodetype) {
        case BTREE_ROOT_NODE:
        case BTREE_INTERIOR_NODE:
            if (dummy.info.numkeys > 0) {
                for (position = 0; position <= dummy.info.numkeys; position++) {
                    errorMessage = dummy.GetPtr(position, ptr);
                    if (errorMessage) { return errorMessage; }
                    if (display_type == BTREE_DEPTH_DOT) {
                        o << node << " -> " << ptr << ";\n";
                    }
                    errorMessage = DisplayInternal(ptr, o, display_type);
                    if (errorMessage) { return errorMessage; }
                }
            }
            return ERROR_NOERROR;
            break;
        case BTREE_LEAF_NODE:
            return ERROR_NOERROR;
            break;
        default:
            if (display_type == BTREE_DEPTH_DOT) {
            } else {
                o << "Unsupported Node Type " << dummy.info.nodetype;
            }
            return ERROR_INSANE;
    }

    return ERROR_NOERROR;
}


ERROR_T BTreeIndex::Display(ostream &o, BTreeDisplayType display_type) const {
    ERROR_T errorMessage;
    if (display_type == BTREE_DEPTH_DOT) {
        o << "digraph tree { \n";
    }
    errorMessage = DisplayInternal(superblock.info.rootnode, o, display_type);
    if (display_type == BTREE_DEPTH_DOT) {
        o << "}\n";
    }
    return ERROR_NOERROR;
}


ERROR_T BTreeIndex::SanityCheckHelper(const SIZE_T &node, const KEY_T &key, const SIZE_T &isLeft) const {
    BTreeNode dummy;
    SIZE_T position;
    KEY_T preKey;
    KEY_T curKey;

    dummy.Unserialize(buffercache, node);

    for (position = 0; position < dummy.info.numkeys; position++) {
        if (position == 0) {
            assert(dummy.GetKey(position, curKey) == ERROR_NOERROR);
            if (isLeft ? key < curKey : (curKey < key || curKey == key)) {
                return ERROR_INSANE;
            }
        } else {
            preKey = curKey;
            assert(dummy.GetKey(position, curKey) == ERROR_NOERROR);
            if ((curKey < preKey) || (isLeft ? key < curKey : (curKey < key || curKey == key))) {
                return ERROR_INSANE;
            }
        }
        if (dummy.info.nodetype != BTREE_LEAF_NODE) {
            SIZE_T leftNode, rightNode;
            assert(dummy.GetPtr(position, leftNode) == ERROR_NOERROR);
            assert(dummy.GetPtr(position + 1, rightNode) == ERROR_NOERROR);
            if (SanityCheckHelper(leftNode, curKey, 1) || SanityCheckHelper(rightNode, curKey, 0)) {
                return ERROR_INSANE;
            }
        }
    }
    return ERROR_NOERROR;
}


ERROR_T BTreeIndex::SanityCheck() const {
    BTreeNode dummy;
    SIZE_T position;
    KEY_T preKey;
    KEY_T curKey;

    dummy.Unserialize(buffercache, superblock.info.rootnode);
    for (position = 0; position < dummy.info.numkeys; position++) {
        if (position == 0) {
            assert(dummy.GetKey(position, curKey) == ERROR_NOERROR);
        } else {
            preKey = curKey;
            assert(dummy.GetKey(position, curKey) == ERROR_NOERROR);
            if (curKey < preKey) {
                return ERROR_INSANE;
            }
        }
        SIZE_T leftNode, rightNode;
        assert(dummy.GetPtr(position, leftNode) == ERROR_NOERROR);
        assert(dummy.GetPtr(position + 1, rightNode) == ERROR_NOERROR);
        if (SanityCheckHelper(leftNode, curKey, 1) || SanityCheckHelper(rightNode, curKey, 0)) {
            return ERROR_INSANE;
        }
    }
    return ERROR_NOERROR;
}


ostream &BTreeIndex::Print(ostream &os) const {
    Display(os, BTREE_DEPTH_DOT);
    return os;
}
