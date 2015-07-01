using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Contracts;

namespace Common
{
    /// <summary>
    /// A general simple cache that stores a bounded number of elements accessed by some keys.
    /// </summary>
    /// <remarks>
    /// Basically a Dictionary plus a bounded-size queue;
    /// the queue allows to move an already inserted element to the front.
    /// 
    /// Everything is constant-time - unlike a full-fledged priority queue:
    /// here any new inserted element is always the freshest, i.e. one with the highest priority.
    /// 
    /// All APIs are synchronous. 
    /// To benefit from any asynchrony, make the lookup externally to the cache and notify 
    /// </remarks>
    public class Cache<Key, Value>
    {
        #region Fields and construction

        /// <summary>
        /// Maximum number of elements allowed in the cache
        /// </summary>
        private readonly int _capacity;

        /// <summary>
        /// This is to detect mis-uses of iteration
        /// </summary>
        private int _version = 0;

        /// <summary>
        /// The comparer used for keys 
        /// </summary>
        private readonly IEqualityComparer<Key> _comparer; 

        /// <summary>
        /// A map that remembers which elements are present and 
        /// what information is attached to them.
        /// </summary>
        /// <remarks>
        /// This could be specialized - TODO
        /// </remarks>
        private Dictionary<Key, Node> _cachedValues;

        // NOTE 
        // Unusually we use a linked representation to allow moving random positions to the front

        /// <summary>
        /// The head of a queue.
        /// </summary>
        private Node _head = null;

        /// <summary>
        /// The tail of the queue - the element that will first be evicted
        /// </summary>
        private Node _tail = null;

        /// <summary>
        /// Doubly-linked queue elements
        /// </summary>
        private class Node
        {
            public Node Previous;
            public Key Key;
            public Value Value;
            public Node Next;
        }

        /// <summary>
        /// The expensive function for which we want to store a cache
        /// </summary>
        private readonly Func<Key, Value> _load;

        /// <summary>
        /// An action that is executed 
        /// </summary>
        private readonly Action<Key, Value> _unload;

        /// <summary>
        /// Create a cache of the given capacity for the specified function.
        /// </summary>
        /// <param name="capacity">Number of keys that are being cached</param>
        /// <param name="load">The function whose results are partially being remembered</param>
        /// <param name="unload">A function called when a cached element gets evicted</param>
        public Cache(int capacity, Func<Key, Value> load, Action<Key, Value> unload = null, IEqualityComparer<Key> comparer = null)
        {
            Contract.Requires(capacity > 2);
            Debug.Assert(capacity > 2);

            _comparer = comparer ?? EqualityComparer<Key>.Default;
            _cachedValues = new Dictionary<Key, Node>(_comparer);

            _capacity = capacity;
            _load = load;
            _unload = unload;
        }

        #endregion

        #region Public API

        /// <summary>
        /// Get the content associated to the specified key. 
        /// Returns the value from the cache if present, 
        /// if not cached the content will be synchronously loaded, cached and returned.
        /// </summary>
        public Value this[Key key]
        {
            get { return Retrieve(key); }
        }

        /// <summary>
        /// Get the number of cached values
        /// </summary>
        internal int Count
        {
            get { return _cachedValues.Count; }
        }

        /// <summary>
        /// Evict all pages from the cache
        /// </summary>
        public void Clear()
        {
            for (var node = _tail; node != null; node = node.Previous)
            {
                _unload(node.Key, node.Value);
            }

            _cachedValues.Clear();
            _head = null;
            _tail = null;
        }

        /// <summary>
        /// True if the key is cached
        /// </summary>
        public bool HasKey(Key key)
        {
            return _cachedValues.ContainsKey(key);
        }

        #endregion

        #region Internals

        private void CheckVersion(int referenceVersion)
        {
            if (_version != referenceVersion)
            {
                throw new Exception("Version change during iteration");
            }
        }

        private Value Retrieve(Key key)
        {
            Contract.Ensures(_comparer.Equals(_head.Key, key));
            Contract.Ensures(_head.Previous == null);
            Contract.Ensures(_cachedValues.ContainsKey(key));
            Contract.Ensures(_cachedValues[key].Equals(Contract.Result<Value>()));
            Contract.Ensures(_cachedValues.Count <= _capacity);

            Node node = null;

            // Fastest cache for most common use:
            // when the same key is accessed multiple times, this makes sure 
            // that we bypass any Dictionary access or anything. 
            if (_head != null && _comparer.Equals(_head.Key, key))
            {
                return _head.Value;
            }
            else if (_cachedValues.TryGetValue(key, out node))
            {
                Debug.Assert(node.Key.Equals(key));
                Debug.Assert(!ReferenceEquals(node, _head)); // also excludes _head == _tail
                _version++;

                // Disconnect the node 

                if (ReferenceEquals(node, _tail))
                {
                    _tail.Previous.Next = null;
                    _tail = _tail.Previous;
                }
                else
                {
                    Debug.Assert(node.Previous != null);
                    Debug.Assert(node.Next != null);

                    node.Previous.Next = node.Next;
                    node.Next.Previous = node.Previous;
                }

                // and put it in head 

                Debug.Assert(_head != null);
                Debug.Assert(_head.Previous == null);

                node.Previous = null;
                node.Next = _head;
                _head.Previous = node;

                _head = node;
                return _head.Value;
            }
            else
            {
                Value value = _load(key);
                UnlockedAdd(key, value);
                return value;
            }
        }

        /// <summary>
        /// Notify the cache that a value has been retrieved
        /// </summary>
        /// <remarks>
        /// Consider exposing as a public method and calling
        /// by hand if any asynchronous retrieval is done externally to the cache
        /// </remarks>
        private void UnlockedAdd(Key key, Value value)
        {
            Contract.Ensures(_comparer.Equals(_head.Key, key));
            Contract.Ensures(_head.Previous == null);
            Contract.Ensures(_cachedValues.ContainsKey(key));
            Contract.Ensures(_cachedValues[key].Equals(Contract.Result<Value>()));
            Contract.Ensures(_cachedValues.Count <= _capacity);

            _version++;

            if (_cachedValues.Count == _capacity)
            {
                Debug.Assert(_capacity > 2);
                _cachedValues.Remove(_tail.Key);
                if (_unload != null)
                    _unload(_tail.Key, _tail.Value);

                // Remove tail
                var reusedNode = _tail;
                _tail.Previous.Next = null;
                _tail = _tail.Previous;

                // insert node with new key
                reusedNode.Previous = null;
                reusedNode.Key = key;
                reusedNode.Value = value;
                reusedNode.Next = _head;

                _head.Previous = reusedNode;
                _head = reusedNode;
            }
            else
            {
                _head = new Node { Previous = null, Key = key, Value = value, Next = _head };

                if (_head.Next != null)
                {
                    Debug.Assert(_head.Next.Previous == null);
                    _head.Next.Previous = _head;
                }

                if (_tail == null)
                    _tail = _head;
            }

            Debug.Assert(_head.Key.Equals(key));
            _cachedValues.Add(key, _head);
        }

        #endregion
    }
}
