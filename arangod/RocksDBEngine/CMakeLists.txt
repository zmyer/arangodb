# -*- mode: CMAKE; -*-

# add sources for rocksdb engine
set(ROCKSDB_SOURCES
  RocksDBEngine/RocksDBAqlFunctions.cpp
  RocksDBEngine/RocksDBBackgroundThread.cpp
  RocksDBEngine/RocksDBCollection.cpp
  RocksDBEngine/RocksDBCommon.cpp
  RocksDBEngine/RocksDBComparator.cpp
  RocksDBEngine/RocksDBCounterManager.cpp
  RocksDBEngine/RocksDBEdgeIndex.cpp
  RocksDBEngine/RocksDBEngine.cpp
  RocksDBEngine/RocksDBExportCursor.cpp
  RocksDBEngine/RocksDBFulltextIndex.cpp
  RocksDBEngine/RocksDBHashIndex.cpp
  RocksDBEngine/RocksDBIncrementalSync.cpp
  RocksDBEngine/RocksDBIndex.cpp
  RocksDBEngine/RocksDBIndexFactory.cpp
  RocksDBEngine/RocksDBIterators.cpp
  RocksDBEngine/RocksDBKey.cpp
  RocksDBEngine/RocksDBKeyBounds.cpp
  RocksDBEngine/RocksDBLogValue.cpp
  RocksDBEngine/RocksDBMethods.cpp
  RocksDBEngine/RocksDBOptimizerRules.cpp
  RocksDBEngine/RocksDBPrimaryIndex.cpp
  RocksDBEngine/RocksDBReplicationCommon.cpp
  RocksDBEngine/RocksDBReplicationContext.cpp
  RocksDBEngine/RocksDBReplicationManager.cpp
  RocksDBEngine/RocksDBReplicationTailing.cpp
  RocksDBEngine/RocksDBRestExportHandler.cpp
  RocksDBEngine/RocksDBRestHandlers.cpp
  RocksDBEngine/RocksDBRestReplicationHandler.cpp
  RocksDBEngine/RocksDBRestWalHandler.cpp
  RocksDBEngine/RocksDBTransactionCollection.cpp
  RocksDBEngine/RocksDBTransactionState.cpp
  RocksDBEngine/RocksDBTypes.cpp
  RocksDBEngine/RocksDBV8Functions.cpp
  RocksDBEngine/RocksDBVPackIndex.cpp
  RocksDBEngine/RocksDBValue.cpp
  RocksDBEngine/RocksDBView.cpp
  RocksDBEngine/GeoIndex/RocksDBGeoIndex.cpp
  RocksDBEngine/GeoIndex/RocksDBGeoIndexImpl.cpp
  RocksDBEngine/GeoIndex/stones.cpp
  RocksDBEngine/GeoIndex/RocksDBGeoIndexImpl.cpp
)
set(ROCKSDB_SOURCES ${ROCKSDB_SOURCES} PARENT_SCOPE)
