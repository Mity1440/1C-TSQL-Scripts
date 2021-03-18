SELECT 
    CASE locks.resource_type
		WHEN N'OBJECT' THEN OBJECT_NAME(locks.resource_associated_entity_id)
		WHEN N'KEY'THEN (SELECT OBJECT_NAME(object_id) FROM sys.partitions WHERE hobt_id = locks.resource_associated_entity_id)
		WHEN N'PAGE' THEN (SELECT OBJECT_NAME(object_id) FROM sys.partitions WHERE hobt_id = locks.resource_associated_entity_id)
		WHEN N'HOBT' THEN (SELECT OBJECT_NAME(object_id) FROM sys.partitions WHERE hobt_id = locks.resource_associated_entity_id)
		WHEN N'RID' THEN (SELECT OBJECT_NAME(object_id) FROM sys.partitions WHERE hobt_id = locks.resource_associated_entity_id)
		ELSE N'Unknown'
    END AS objectName,
    CASE locks.resource_type
		WHEN N'KEY' THEN (SELECT indexes.name 
							FROM sys.partitions JOIN sys.indexes 
								ON partitions.object_id = indexes.object_id AND partitions.index_id = indexes.index_id
							WHERE partitions.hobt_id = locks.resource_associated_entity_id)
		ELSE N'Unknown'
    END AS IndexName,
    locks.resource_type,
	DB_NAME(locks.resource_database_id) AS database_name,
	locks.resource_description,
	locks.resource_associated_entity_id,
	locks.request_mode
FROM sys.dm_tran_locks AS locks
	--WHERE locks.resource_database_id = DB_ID(N'DB_Name')

--просмотр заблокированной строки индекса
SELECT 
	%%lockres%% AS lockres,
	*
FROM
	dbo.table_name WITH (INDEX (index_name) NOLOCK))
	
--просмотр содержимого страницы
DBCC TRACEON(3604)
DBCC PAGE('database_name', 1, 423, 3) WITH TABLERESULTS

--просмотр размещения базы по страницам

SELECT
	OBJECT_NAME(pages.object_id) AS object,
	*
FROM
	sys.dm_db_database_page_allocations(DB_ID('database_name'), NULL ,NULL,NULL,'DETAILED') AS pages