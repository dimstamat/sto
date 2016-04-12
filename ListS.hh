// ListS.hh
// Singly-linked list with StoSnapshot support
// Supports an STL map-style interface
// XXX no iterator for now

#include "TaggedLow.hh"
#include "Interface.hh"
#include "Transaction.hh"
#include "Snapshot.hh"

template <typename T>
class DefaultCompare {
public:
    int operator()(const T& t1, const T& t2) {
        if (t1 < t2)
            return -1;
        return t2 < t1 ? 1 : 0;
    }
};

template <typename K, typename V, typename Compare=DefaultCompare<K>>
class List;

template <typename K, typename V, typename Compare>
class ListCursor;

template <typename K, typename V>
class ListProxy;

template <typename K, typename V>
class ListNode {
public:
    typedef StoSnapshot::sid_type sid_type;
    ListNode(const K& key, const V& val) : key(key), val(val) {}

    bool is_valid() {
    }

    K key;
    V val;
};

template <typename K, typename V>
class ListLink {
public:
    typedef StoSnapshot::ObjectID<ListNode<K, V>, ListLink<K, V>> oid_type;
    static constexpr uintptr_t use_base = 0x1;
    oid_type next_id;

    ListLink() : next_id(use_base) {}
    ListLink(oid_type next) : next_id(next) {}
};

// proxy type for List::operator[]
template <typename K, typename V>
class ListProxy {
public:
    typedef ListNode<K, V> node_type;
    typedef ListLink<K, V> link_type;
    typedef StoSnapshot::ObjectID<node_type, link_type> oid_type;
    typedef StoSnapshot::NodeWrapper<node_type, link_type> wrapper_type;

    explicit ListProxy(oid_type ins_oid) noexcept : oid(ins_oid) {}

    // transactional non-snapshot read
    operator V() {
        auto item = Sto::item(this, oid);
        if (item.has_write()) {
            return item.template write_value<V>();
        } else {
            return atomic_rootlv_value_by_oid(item, oid);
        }
    }

    // transactional assignment
    ListProxy& operator=(const V& other) {
        Sto::item(this, oid).add_write(other);
        return *this;
    }

    ListProxy& operator=(const ListProxy& other) {
        Sto::item(this, oid).add_write((V)other);
        return *this;
    }
private:
    oid_type oid;
};

// always sorted, no duplicates
template <typename K, typename V, typename Compare>
class List : public TObject {
public:
    typedef ListNode<K, V> node_type;
    typedef ListLink<K, V> link_type;
    typedef StoSnapshot::ObjectID<node_type, link_type> oid_type;
    typedef StoSnapshot::NodeWrapper<node_type, link_type> wrapper_type;
    typedef StoSnapshot::sid_type sid_type;
    typedef ListCursor<K, V, Compare> cursor_type;

    friend cursor_type;

    enum class TxnStage {execution, commit, cleanup, none};

    static constexpr uintptr_t list_key = 0;
    static constexpr uintptr_t size_key = 1;

    static constexpr TransactionTid::type poisoned_bit = TransactionTid::user_bit;
    static constexpr TransItem::flags_type insert_tag = TransItem::user0_bit;
    static constexpr TransItem::flags_type delete_tag = TransItem::user0_bit<<1;

    List(Compare comp = Compare())
    : head_id_(nullptr), listlock_(0), listversion_(0), comp_(comp) {}

    std::pair<bool, V> nontrans_find(const K& k, sid_type sid) {
        cursor_type cursor(*this);
        bool found = cursor.find(k, sid);
        return std::make_pair(found, found ? cursor.last_node->val : V());
    }

    V atomic_rootlv_value_by_oid(TransItem& item, oid_type oid) {
        auto bp = oid.base_ptr();
        TVersion& r_ver = bp->version();
        return bp->atomic_root_wrapper(item, r_ver)->node().val;
    }

    std::pair<bool, V> trans_find(const K& k) {
        sid_type sid = Sto::active_sid();
        if (sid != Sto::initialized_tid()) {
            // snapshot reads are safe as nontrans
            return nontrans_find(k, sid);
        }

        // being conservative: making sure listversion_ doesn't change
        std::pair<bool, V> ret_pair(false, V());
        cursor_type cursor(*this);
        TVersion lv = listversion_;
        fence();

        bool found = cursor.find(k, sid);
        
        if (found) {
            auto item = Sto::item(this, cursor.last_oid);
            TVersion& r_ver = cursor.last_oid.base_ptr()->version();
            if (is_poisoned(r_ver) && !has_insert(item)) {
                Sto::abort();
            }
            if (has_delete(item)) {
                return ret_pair;
            }
            ret_pair.first = true;
            ret_pair.second = atomic_rootlv_value_by_oid(item, cursor.last_oid);
        } else {
            Sto::item(this, list_key).observe(lv);
        }
        return ret_pair;
    }

    ListProxy<K, V> operator[](const K& key) {
        return ListProxy<K, V>(insert_position(key));
    }

    // "STAMP"-ish insert XXX TODO
    bool trans_insert(const K& key, const V& value) {
        lock(listlock_);
        auto results = _insert(key, V());
        unlock(listlock_);

        auto item = Sto::item(this,
                        reinterpret_cast<wrapper_type*>(results.second)->oid);
        if (results.first) {
            // successfully inserted
            auto litem = Sto::item(this, list_key);
            litem.add_write(0);

            item.add_write(value);
            item.add_flags(insert_tag);
            return true;
        }

        // not inserted if we fall through
        node_type* node = results.second;
        TVersion ver = node->version();
        fence();
        bool poisoned = !node->is_valid();
        if (!item.has_write() && poisoned) {
            Sto::abort();
        } else if (has_delete(item)) {
            item.clear_flags(delete_tag);
            if (poisoned) {
                // THIS txn inserted this node, now cleanup
                item.add_flags(insert_tag);
            }
            item.add_write(value);
            return true;
        } else if (has_insert(item)) {
            return false;
        }

        item.observe(ver);
        return false;
    }

    // XXX TODO
    bool trans_erase(const K& key) {
        TVersion lv = listversion_;
        fence();

        node_type* n = _find(key, 0);
        if (n == nullptr) {
            auto item = Sto::item(this, list_key);
            item.observe(lv);
            return false;
        }

        bool poisoned = !n->is_valid();
        auto item = Sto::item(this,
                        reinterpret_cast<wrapper_type*>(n)->oid);
        if (!has_write(item) && poisoned) {
            Sto::abort();
        }
        if (has_delete(item)) {
            return false;
        } else if (has_insert(item)) {
            // deleting a key inserted by THIS transaction
            // defer cleanups to cleanup()
            item.remove_read().add_flags(delete_tag);
            // XXX insert_tag && delete_tag means needs cleanup after committed
        } else {
            // increment list version
            auto litem = Sto::item(this, list_key);
            litem.add_write(0);

            item.assign_flags(delete_tag);
            item.add_write(0);
            item.observe(lv);
        }
        return true;
    }

    bool lock(TransItem& item, Transaction&) override {
        uintptr_t n = item.key<uintptr_t>();
        if (n == list_key) {
            return listversion_.try_lock();
        } else if (!has_insert(item)) {
            return oid_type(n).base_ptr()->node().try_lock();
        }
        return true;
    }

    void unlock(TransItem& item) override {
        uintptr_t n = item.key<uintptr_t>();
        if (n == list_key) {
            return listversion_.unlock();
        } else if (!has_insert(item)) {
            oid_type(n).base_ptr()->node().unlock();
        }
    }

    bool check(TransItem& item, Transaction&) override {
        uintptr_t n = item.key<uintptr_t>();
        if (n == list_key) {
            auto lv = listversion_;
            return lv.check_version(item.template read_value<TVersion>());
        }
        node_type *nn = &(reinterpret_cast<oid_type>(n).base_ptr()->node());
        if (!nn->is_valid()) {
            return has_insert(item);
        }
        return nn->version().check_version(item.template read_value<TVersion>());
    }

    void install(TransItem& item, Transaction& t) override {
        if (item.key<uintptr_t>() == list_key) {
            listversion_.set_version(t.commit_tid());
            return;
        }
        oid_type oid = item.key<oid_type>();
        auto bp = oid.base_ptr();

        // copy-on-write for both deletes and updates
        if (bp->is_immutable())
            bp->save_copy_in_history();

        if (has_delete(item)) {
            bp->set_unlinked();
            reinterpret_cast<wrapper_type*>(&bp->node())->s_sid = 0;
        } else {
            bp->node().val.write(item.template write_value<V>());
            if (has_insert(item)) {
                sid_type& s_sid = reinterpret_cast<wrapper_type*>(&bp->node())->s_sid;
                assert(s_sid == 0);
                s_sid = Sto::GSC_snapshot();
            }
            bp->node().set_version_unlock(t.commit_tid());
        }
    }

    void cleanup(TransItem& item, bool committed) override {
        if (has_insert(item)) {
            if (!committed || (committed && has_delete(item))) {
                lock(listlock_);
                _remove(item.key<oid_type>().base_ptr()->node().key, TxnStage::cleanup);
                unlock(listlock_);
            }
        }
    }

private:
    // returns the node the key points to, or abort if observes uncommitted state
    oid_type insert_position(const K& key) {
        lock(listlock_);
        auto results = _insert(key, V());
        unlock(listlock_);

        if (results.first) {
            return results.second;
        }
        auto item = Sto::item(this, results.second);
        auto bp = results.second.base_ptr();
        bool poisoned = is_poisoned(bp->version());
        if (!item.has_write() && poisoned) {
            Sto::abort();
        } else if (has_delete(item)) {
            item.clear_flags(delete_tag);
            if (poisoned) {
                item.add_flags(insert_tag);
            }
            item.add_write(V());
        }
        return results.second;
    }

/*
    node_type* _find(const K& key, sid_type sid) {
        node_type* cur = next_snapshot(head_id_, sid);
        bool cur_history = false;
        while (cur) {
            // to see if @cur is part of the history list
            cur_history = reinterpret_cast<wrapper_type*>(cur)->c_sid != 0;

            int c = comp_(cur->key, key);
            if (c == 0) {
                return cur;
            } else if (c > 0) {
                return nullptr;
            }

            // skip nodes that are not part of the snapshot we look for
            oid_type nid = cur->next_id;
            node_type* next = next_snapshot(nid, sid);

            // direct-link two immutable nodes if detected
            if (cur_history &&
                (!next || reinterpret_cast<wrapper_type*>(next)->c_sid)) {
                cur->next_id.set_direct_link(next);
            }

            cur = next;
        }
        return cur;
    }
*/

    // XXX note to cleanup: never structurally remove a node until history is empty
    // needs lock
    std::pair<bool, oid_type> _insert(const K& key, const V& value) {
        oid_type prev_obj = nullptr;
        oid_type cur_obj = head_id_;

        cursor_type cursor(*this);

        // all the magic in one line :D
        bool found = cursor.find(key, Sto::initialized_tid());

        auto cbp = cursor.last_oid.base_ptr();
        auto node = cursor.last_node;
        if (found && cbp->is_unlinked()) {
            // found an unlinked object with the same key, reuse this object
            mark_poisoned_init(cbp->version());
            new (node) node_type(key, value);
            cbp->clear_unlinked();

            return std::make_pair(true, cursor.last_oid);
        } else if (found) {
            return std::make_pair(false, cursor.last_oid);
        }

        // if we fall through we have to insert a new object
        // prev_oid, last_oid is the insertion point

        auto new_obj = StoSnapshot::new_object<node_type, link_type>();
        auto new_bp = new_obj.first.base_ptr();
        new_bp->links.next_id = cursor.last_oid;
        mark_poisoned_init(new_bp->version());
        new (new_obj.second) node_type(key, value);

        if (!cursor.prev_oid.is_null()) {
            cursor.prev_oid.base_ptr()->links.next_id = new_obj.first;
        } else {
            head_id_ = new_obj.first;
        }

        return std::make_pair(true, new_obj.first);
    }

    // needs lock XXX TODO
    bool _remove(const K& key, TxnStage stage) {
        oid_type prev_obj = nullptr;
        oid_type cur_obj = head_id_;
        while (!cur_obj.is_null()) {
            node_type* cur = cur_obj.deref(0).second;
            auto bp = cur_obj.base_ptr();
            if (comp_(cur->key, key) == 0) {
                if (stage == TxnStage::commit) {
                    assert(cur->is_valid());
                    assert(!bp->is_unlinked());
                    // never physically unlink the object since it must contain snapshots!
                    // (the one we just saved!)
                    bp->save_copy_in_history();
                    cur->mark_invalid();
                    bp->set_unlinked();
                } else if (stage == TxnStage::cleanup) {
                    assert(!cur->is_valid());
                    assert(!bp->is_unlinked());
                    if (bp->history_is_empty()) {
                        // unlink the entire object if that's all we've instered
                        _do_unlink(prev_obj, cur_obj, stage);
                    } else {
                        cur->mark_invalid();
                        bp->set_unlinked();
                    }
                } else if (stage == TxnStage::none) {
                    _do_unlink(prev_obj, cur_obj, stage);
                }
                return true;
            }
            prev_obj = cur_obj;
            cur_obj = cur->next_id;
        }
        return false;
    }

    // XXX TODO
    inline void _do_unlink(oid_type& prev_obj, oid_type cur_obj, TxnStage stage) {
        node_type* cur = cur_obj.deref(0).second;
        if (!prev_obj.is_null()) {
            prev_obj.deref(0).second->next_id = cur->next_id;
        } else {
            head_id_ = cur->next_id;
        }

        auto bp = cur_obj.base_ptr();
        if (stage == TxnStage::none) {
            delete cur;
            delete bp;
        } else {
            Transaction::rcu_delete(cur);
            Transaction::rcu_delete(bp);
        }
    }

    // skip nodes that are not part of the snapshot we look for
    node_type* next_snapshot(oid_type start, sid_type sid) {
        oid_type cur_obj = start;
        node_type* ret = nullptr;
        while (sid != Sto::initialized_tid()) {
            if (cur_obj.is_null()) {
                break;
            }
            // ObjectID::deref returns <next, root(if next not found given sid)>
            auto n = cur_obj.deref(sid);
            if (!n.first) {
                // object id exists but no snapshot is found at sid
                // this should cover nodes that are more up-to-date than sid (when doing a snapshot read)
                // or when the node is unlinked (when doing a non-snapshot read, or sid==0)
                cur_obj = cur_obj.base_ptr()->links.next_id;
            } else {
                // snapshot node found
                ret = n.first;
                break;
            }
        }
        return ret;
    }

    static inline void mark_poisoned(TVersion& ver) {
        TVersion newv = ver | poisoned_bit;
        fence();
        ver = newv;
    }

    static inline void mark_poisoned_init(TVersion& ver) {
        ver = (Sto::initialized_tid() | (poisoned_bit | TransactionTid::lock_bit | TThread::id()));
    }

    static inline bool is_poisoned(TVersion& ver) {
        return (ver.value() & poisoned_bit);
    }

    static void lock(TVersion& ver) {
        ver.lock();
    }

    static void unlock(TVersion& ver) {
        ver.unlock();
    }

    static bool has_insert(const TransItem& item) {
        return item.flags() & insert_tag;
    }

    static bool has_delete(const TransItem& item) {
        return item.flags() & delete_tag;
    }

    oid_type head_id_;
    TVersion listlock_;
    TVersion listversion_;
    Compare comp_;
};

// ListCursor is read-only and can be used lock-free if not looking for insertion points
template <typename K, typename V, typename Compare>
class ListCursor {
public:
    typedef List<K, V, Compare> list_type;
    typedef ListNode<K, V> node_type;
    typedef ListLink<K, V> link_type;
    typedef StoSnapshot::ObjectID<node_type, link_type> oid_type;
    typedef StoSnapshot::NodeWrapper<node_type, link_type> wrapper_type;
    typedef StoSnapshot::sid_type sid_type;

    oid_type prev_oid;
    oid_type last_oid;
    wrapper_type* prev_node;
    wrapper_type* last_node;

    ListCursor(list_type& l) : last_oid(nullptr, false), prev_oid(nullptr, false),
        last_node(nullptr), prev_node(nullptr), list(l) {}

    void reset() {
        prev_oid = last_oid = nullptr;
        prev_node = last_node = nullptr;
    }

    // find the node containing the key at the given sid
    // only searches at the root level if @sid == Sto::initialized_tid()
    // this will return unlinked nodes!
    bool find(const K& key, sid_type sid) {
        bool in_snapshot = next_snapshot(list.head_id_, sid);
        while (last_node) {
            int c = list.comp_(last_node->node().key, key);
            if (c == 0) {
                return true;
            } else if (c > 0) {
                // prev_oid, last_oid indicates (structural) insertion point!
                return false;
            }

            // skip nodes not part of the snapshot we look for
            oid_type nid = last_oid.base_ptr()->links.next_id;
            oid_type cur_oid = last_oid;
            wrapper_type* cur = last_node;
            bool next_in_snapshot = next_snapshot(nid, sid);

            // lazily fix up direct links when possible
            if (in_snapshot && (!last_node || next_in_snapshot)) {
                assert((uintptr_t)cur->links.next_id.base_ptr() == link_type::use_base);
                last_node->links.next_id.set_direct_link(last_node);
            }

            in_snapshot = next_in_snapshot;
        }

        return false;
    }
private:
    // skip nodes that are not part of the snapshot (based on sid) we look for
    // modifies last_oid and last_node: not found if last_node == nullptr
    // return value indicates whether the returned node is part of a history list (immutable)
    bool next_snapshot(oid_type& start, sid_type sid) {
        prev_oid = start;
        auto cbp = prev_oid.base_ptr();

        // always return false if we are not doing a snapshot search
        if (sid == Sto::initialized_tid()) {
            cbp = prev_oid.base_ptr();
            if (!prev_oid.is_null()) {
                prev_node = cbp->root_wrapper();
                // no need to skip unlinked nodes
                last_oid = cbp->links.next_id;
                if (last_oid.is_null()) {
                    last_node = nullptr;
                    return false;
                } else {
                    cbp = last_oid.base_ptr();
                    last_node = cbp->root_wrapper();
                }
            } else {
                this->reset();
                return false;
            }

            return false;
        }

        // we are doing a snapshot search here; need to search through history in this case
        // not using prev_* fields here since we can't insert
        last_oid = start;
        while (!last_oid.is_null()) {
            cbp = last_oid.base_ptr();

            // ObjectID::deref searches through all available history (include root level)
            // and returns the wrapper containing the matching sid, if any
            last_node = last_oid.deref(sid);
            if (last_node == nullptr) {
                // ObjectID exists but no snapshot is found at sid
                last_oid = cbp->links.next_id;
            } else {
                // a wrapper is found with the exact match
                // the corresponding node is "in snapshot" (immutable), thus returning true
                return true;
            }
        }

        this->reset();
        return false;
    }

    list_type& list;
};
