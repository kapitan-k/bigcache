package bigcache

import (
	"fmt"
	"io/ioutil"
	//"os"
	. "github.com/kapitan-k/bigcache/buffer"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var mmapBufferCreator *SeqentialMmapFileBufferCreator

func TestSeqentialMmapFileBufferCreatorNew(t *testing.T) {
	var err error
	var buffer Buffer
	var tmpDirName string
	tmpDirName, err = ioutil.TempDir("", "iox")
	assert.NoError(t, err)

	mmapBufferCreator = NewSeqentialMmapFileBufferCreator(tmpDirName)
	buffer, err = mmapBufferCreator.NewBuffer(1024 * 1024)
	// then
	assert.NoError(t, err)
	assert.NotNil(t, buffer)

	err = buffer.Close()
	assert.NoError(t, err)
}

func TestMmapWriteAndGetOnCache(t *testing.T) {

	c := DefaultConfig(5 * time.Second)
	c.BufferCreator = mmapBufferCreator
	// given
	cache, _ := NewBigCache(c)
	value := []byte("value")

	// when
	cache.Set("key", value)
	cachedValue, err := cache.Get("key")

	// then
	assert.NoError(t, err)
	assert.Equal(t, value, cachedValue)
}

func TestMmapConstructCacheWithDefaultHasher(t *testing.T) {

	// given
	cache, _ := NewBigCache(Config{
		Shards:             16,
		LifeWindow:         5 * time.Second,
		MaxEntriesInWindow: 10,
		MaxEntrySize:       256,
		BufferCreator:      mmapBufferCreator,
	})

	assert.IsType(t, fnv64a{}, cache.hash)
}

func TestMmapWillReturnErrorOnInvalidNumberOfPartitions(t *testing.T) {

	// given
	cache, error := NewBigCache(Config{
		Shards:             18,
		LifeWindow:         5 * time.Second,
		MaxEntriesInWindow: 10,
		MaxEntrySize:       256,
		BufferCreator:      mmapBufferCreator,
	})

	assert.Nil(t, cache)
	assert.Error(t, error, "Shards number must be power of two")
}

func TestMmapEntryNotFound(t *testing.T) {

	// given
	cache, _ := NewBigCache(Config{
		Shards:             16,
		LifeWindow:         5 * time.Second,
		MaxEntriesInWindow: 10,
		MaxEntrySize:       256,
		BufferCreator:      mmapBufferCreator,
	})

	// when
	_, err := cache.Get("nonExistingKey")

	// then
	assert.EqualError(t, err, "Entry \"nonExistingKey\" not found")
}

func TestMmapTimingEviction(t *testing.T) {

	// given
	clock := mockedClock{value: 0}
	cache, _ := newBigCache(Config{
		Shards:             1,
		LifeWindow:         time.Second,
		MaxEntriesInWindow: 1,
		MaxEntrySize:       256,
		BufferCreator:      mmapBufferCreator,
	}, &clock)

	// when
	cache.Set("key", []byte("value"))
	clock.set(5)
	cache.Set("key2", []byte("value2"))
	_, err := cache.Get("key")

	// then
	assert.EqualError(t, err, "Entry \"key\" not found")
}

func TestMmapTimingEvictionShouldEvictOnlyFromUpdatedShard(t *testing.T) {

	// given
	clock := mockedClock{value: 0}
	cache, _ := newBigCache(Config{
		Shards:             4,
		LifeWindow:         time.Second,
		MaxEntriesInWindow: 1,
		MaxEntrySize:       256,
		BufferCreator:      mmapBufferCreator,
	}, &clock)

	// when
	cache.Set("key", []byte("value"))
	clock.set(5)
	cache.Set("key2", []byte("value 2"))
	value, err := cache.Get("key")

	// then
	assert.NoError(t, err, "Entry \"key\" not found")
	assert.Equal(t, []byte("value"), value)
}

func TestMmapCleanShouldEvictAll(t *testing.T) {

	// given
	cache, _ := NewBigCache(Config{
		Shards:             4,
		LifeWindow:         time.Second,
		CleanWindow:        time.Second,
		MaxEntriesInWindow: 1,
		MaxEntrySize:       256,
		BufferCreator:      mmapBufferCreator,
	})

	// when
	cache.Set("key", []byte("value"))
	<-time.After(3 * time.Second)
	value, err := cache.Get("key")

	// then
	assert.EqualError(t, err, "Entry \"key\" not found")
	assert.Equal(t, value, []byte(nil))
}

func TestMmapOnRemoveCallback(t *testing.T) {

	// given
	clock := mockedClock{value: 0}
	onRemoveInvoked := false
	onRemove := func(key string, entry []byte) {
		onRemoveInvoked = true
		assert.Equal(t, "key", key)
		assert.Equal(t, []byte("value"), entry)
	}
	cache, _ := newBigCache(Config{
		Shards:             1,
		LifeWindow:         time.Second,
		MaxEntriesInWindow: 1,
		MaxEntrySize:       256,
		OnRemove:           onRemove,
		BufferCreator:      mmapBufferCreator,
	}, &clock)

	// when
	cache.Set("key", []byte("value"))
	clock.set(5)
	cache.Set("key2", []byte("value2"))

	// then
	assert.True(t, onRemoveInvoked)
}

func TestMmapCacheLen(t *testing.T) {

	// given
	cache, _ := NewBigCache(Config{
		Shards:             8,
		LifeWindow:         time.Second,
		MaxEntriesInWindow: 1,
		MaxEntrySize:       256,
		BufferCreator:      mmapBufferCreator,
	})
	keys := 1337
	// when

	for i := 0; i < keys; i++ {
		cache.Set(fmt.Sprintf("key%d", i), []byte("value"))
	}

	// then
	assert.Equal(t, keys, cache.Len())
}

func TestMmapCacheReset(t *testing.T) {

	// given
	cache, _ := NewBigCache(Config{
		Shards:             8,
		LifeWindow:         time.Second,
		MaxEntriesInWindow: 1,
		MaxEntrySize:       256,
		BufferCreator:      mmapBufferCreator,
	})
	keys := 1337

	// when
	for i := 0; i < keys; i++ {
		cache.Set(fmt.Sprintf("key%d", i), []byte("value"))
	}

	// then
	assert.Equal(t, keys, cache.Len())

	// and when
	cache.Reset()

	// then
	assert.Equal(t, 0, cache.Len())

	// and when
	for i := 0; i < keys; i++ {
		cache.Set(fmt.Sprintf("key%d", i), []byte("value"))
	}

	// then
	assert.Equal(t, keys, cache.Len())
}

func TestMmapIterateOnResetCache(t *testing.T) {

	// given
	cache, _ := NewBigCache(Config{
		Shards:             8,
		LifeWindow:         time.Second,
		MaxEntriesInWindow: 1,
		MaxEntrySize:       256,
		BufferCreator:      mmapBufferCreator,
	})
	keys := 1337

	// when
	for i := 0; i < keys; i++ {
		cache.Set(fmt.Sprintf("key%d", i), []byte("value"))
	}
	cache.Reset()

	// then
	iterator := cache.Iterator()

	assert.Equal(t, false, iterator.SetNext())
}

func TestMmapGetOnResetCache(t *testing.T) {

	// given
	cache, _ := NewBigCache(Config{
		Shards:             8,
		LifeWindow:         time.Second,
		MaxEntriesInWindow: 1,
		MaxEntrySize:       256,
		BufferCreator:      mmapBufferCreator,
	})
	keys := 1337

	// when
	for i := 0; i < keys; i++ {
		cache.Set(fmt.Sprintf("key%d", i), []byte("value"))
	}

	cache.Reset()

	// then
	value, err := cache.Get("key1")

	assert.Equal(t, err.Error(), "Entry \"key1\" not found")
	assert.Equal(t, value, []byte(nil))
}

func TestMmapEntryUpdate(t *testing.T) {

	// given
	clock := mockedClock{value: 0}
	cache, _ := newBigCache(Config{
		Shards:             1,
		LifeWindow:         6 * time.Second,
		MaxEntriesInWindow: 1,
		MaxEntrySize:       256,
		BufferCreator:      mmapBufferCreator,
	}, &clock)

	// when
	cache.Set("key", []byte("value"))
	clock.set(5)
	cache.Set("key", []byte("value2"))
	clock.set(7)
	cache.Set("key2", []byte("value3"))
	cachedValue, _ := cache.Get("key")

	// then
	assert.Equal(t, []byte("value2"), cachedValue)
}

func TestMmapOldestEntryDeletionWhenMaxCacheSizeIsReached(t *testing.T) {

	// given
	cache, _ := NewBigCache(Config{
		Shards:             1,
		LifeWindow:         5 * time.Second,
		MaxEntriesInWindow: 1,
		MaxEntrySize:       1,
		HardMaxCacheSize:   1,
		BufferCreator:      mmapBufferCreator,
	})

	// when
	cache.Set("key1", blob('a', 1024*400))
	cache.Set("key2", blob('b', 1024*400))
	cache.Set("key3", blob('c', 1024*800))

	_, key1Err := cache.Get("key1")
	_, key2Err := cache.Get("key2")
	entry3, _ := cache.Get("key3")

	// then
	assert.EqualError(t, key1Err, "Entry \"key1\" not found")
	assert.EqualError(t, key2Err, "Entry \"key2\" not found")
	assert.Equal(t, blob('c', 1024*800), entry3)
}

func TestMmapRetrievingEntryShouldCopy(t *testing.T) {

	// given
	cache, _ := NewBigCache(Config{
		Shards:             1,
		LifeWindow:         5 * time.Second,
		MaxEntriesInWindow: 1,
		MaxEntrySize:       1,
		HardMaxCacheSize:   1,
		BufferCreator:      mmapBufferCreator,
	})
	cache.Set("key1", blob('a', 1024*400))
	value, key1Err := cache.Get("key1")

	// when
	// override queue
	cache.Set("key2", blob('b', 1024*400))
	cache.Set("key3", blob('c', 1024*400))
	cache.Set("key4", blob('d', 1024*400))
	cache.Set("key5", blob('d', 1024*400))

	// then
	assert.Nil(t, key1Err)
	assert.Equal(t, blob('a', 1024*400), value)
}

func TestMmapEntryBiggerThanMaxShardSizeError(t *testing.T) {

	// given
	cache, _ := NewBigCache(Config{
		Shards:             1,
		LifeWindow:         5 * time.Second,
		MaxEntriesInWindow: 1,
		MaxEntrySize:       1,
		HardMaxCacheSize:   1,
		BufferCreator:      mmapBufferCreator,
	})

	// when
	err := cache.Set("key1", blob('a', 1024*1025))

	// then
	assert.EqualError(t, err, "Entry is bigger than max shard size.")
}

func TestMmapHashCollision(t *testing.T) {

	ml := &mockedLogger{}
	// given
	cache, _ := NewBigCache(Config{
		Shards:             16,
		LifeWindow:         5 * time.Second,
		MaxEntriesInWindow: 10,
		MaxEntrySize:       256,
		Verbose:            true,
		Hasher:             hashStub(5),
		Logger:             ml,
		BufferCreator:      mmapBufferCreator,
	})

	// when
	cache.Set("liquid", []byte("value"))
	cachedValue, err := cache.Get("liquid")

	// then
	assert.NoError(t, err)
	assert.Equal(t, []byte("value"), cachedValue)

	// when
	cache.Set("costarring", []byte("value 2"))
	cachedValue, err = cache.Get("costarring")

	// then
	assert.NoError(t, err)
	assert.Equal(t, []byte("value 2"), cachedValue)

	// when
	cachedValue, err = cache.Get("liquid")

	// then
	assert.Error(t, err)
	assert.Nil(t, cachedValue)

	assert.NotEqual(t, "", ml.lastFormat)
}
