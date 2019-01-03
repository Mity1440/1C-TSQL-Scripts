DECLARE @sql_query_text_doc as nvarchar(max),
		@sql_query_text_Reference as nvarchar(max),
		@sql_query_text as nvarchar(max),
		@sql_text as nvarchar(max),
		@attr_name as nvarchar(max),
		@i as int,
		@K as int,
		@j as int,
		@jj as int,
		@attr_prec as nvarchar(max),
		@attr_scale as nvarchar(max),
		@schema_name as nvarchar(max),
		@tablename as nvarchar(max),
		@DBName as nvarchar(max)

---таблица содержит тексты запросов определяющих объектные сущности 1С (запрос к документам, к регистрам бухгалтери,.....)
DECLARE @counter table(_id int, _query_text nvarchar(max))
-- опишем таблицы сущностей Документы
SET @sql_text = N'Select t.Name as Name,SC.Name as schema_name 
					From '+QUOTENAME(@DBName)+N'.Sys.tables as t
					left join Sys.schemas SC 
					ON t.schema_id = SC.schema_id
--					Where t.Name like ''_Document%'' and CHARINDEX(''Chng'',t.Name)=0 and CHARINDEX(''Journal'',t.Name)=0
					Where t.Name like ''_Document%'' and CHARINDEX(''Chng'',t.Name)=0
					Order by t.name';
INSERT INTO @counter(_id, _query_text)
SELECT 1, @sql_text
------ опишем таблицы сущностей Регистры Бухгалтери
SET @sql_text = N'Select t.Name as Name,SC.Name as schema_name 
				From '+QUOTENAME(@DBName)+N'.Sys.tables as t
				left join Sys.schemas SC 
				ON t.schema_id = SC.schema_id
				Where t.Name like ''_Acc[0-9]%'' and CHARINDEX(''Chng'',t.Name)=0 
				UNION ALL
				Select t.Name as Name,SC.Name as schema_name  
				From '+QUOTENAME(@DBName)+N'.Sys.tables as t
				left join Sys.schemas SC 
				ON t.schema_id = SC.schema_id
				Where t.Name like ''_AccRg%'' and CHARINDEX(''Opt'',t.Name)=0
				order by t.name'
INSERT INTO @counter(_id, _query_text)
SELECT 2, @sql_text
------ опишем таблицы сущностей Регистры накопления
SET @sql_text = N'Select t.Name as Name,SC.Name as schema_name
				From '+QUOTENAME(@DBName)+N'.Sys.tables as t
				left join Sys.schemas SC 
				ON t.schema_id = SC.schema_id
				Where t.Name like ''_AccumRg[0-9]%''
				UNION ALL
				Select t.Name as Name,SC.Name as schema_name 
				From '+QUOTENAME(@DBName)+N'.Sys.tables as t
				left join Sys.schemas SC 
				ON t.schema_id = SC.schema_id
				Where t.Name like ''_AccumRgT%'' 
				order by t.name'
INSERT INTO @counter(_id, _query_text)
SELECT 3, @sql_text
-- опишем таблицы сущностей Регистры сведений
SET @sql_text = N'Select t.Name as Name,SC.Name as schema_name 
				From '+QUOTENAME(@DBName)+N'.Sys.tables as t
				left join Sys.schemas SC 
				ON t.schema_id = SC.schema_id
				Where t.Name like ''_InfoRg%'' 
				and CHARINDEX(''Chng'',t.Name)=0 
				and CHARINDEX(''Opt'',t.Name)=0
				Order by t.name'
INSERT INTO @counter(_id, _query_text)
SELECT 4, @sql_text
-- опишем таблицы сущностей Регистр расчета
SET @sql_text = N'Select t.Name as Name,SC.Name as schema_name 
				From '+QUOTENAME(@DBName)+N'.Sys.tables as t
				left join Sys.schemas SC 
				ON t.schema_id = SC.schema_id
				Where t.Name like ''_CRg%'' 
				and CHARINDEX(''Chng'',t.Name)=0 
				and CHARINDEX(''Opt'',t.Name)=0 
				and CHARINDEX(''Recalc'',t.Name)=0  
				Order by t.name'
INSERT INTO @counter(_id, _query_text)
SELECT 5, @sql_text
-- Всего документов
Select @jj = Count(*) from @counter 
 
-------------------------------------------------------------------------------------------------------------------
--Шаблоны текстов запросов
SET @sql_query_text_doc =
							N'SELECT  
							MetaSchema.COLUMN_NAME as Name,
							MetaSchema.NUMERIC_PRECISION as prec,
							MetaSchema.NUMERIC_SCALE as scale
							FROM '+QUOTENAME(@DBName)+N'.INFORMATION_SCHEMA.COLUMNS as MetaSchema -- отсюда возьмем имя, длину  и точность
							Where 
							TABLE_NAME = @tablename
							and COLUMN_NAME like ''_Fld%'' 
							and DATA_TYPE = ''numeric''
							and not EXISTS (    
							SELECT  
							MetaSchema.COLUMN_NAME as Name,
							ti.type
							FROM '+QUOTENAME(@DBName)+N'.sys.tables as t
							INNER JOIN '+QUOTENAME(@DBName)+N'.sys.columns as tc -- отсюда возьмем id колонки 
								on t.object_id = tc.object_id 
								and MetaSchema.Column_name = tc.name  
							left JOIN '+QUOTENAME(@DBName)+N'.sys.index_columns as it -- отсюда возьмем id колонки индекса
								on it.object_id = tc.object_id and it.column_id = tc.column_id
							left JOIN '+QUOTENAME(@DBName)+N'.sys.indexes as ti -- отсюда возьмем id колонки 
								on ti.object_id = tc.object_id and it.index_id = ti.index_id 
							WHERE	
								t.name = MetaSchema.TABLE_NAME
								and ti.type =1)';
-------------------------------------------------------------------------------------------------------------------									
Set @sql_query_text_Reference = N'Update @tablename Set '; 
---------------------------------------Обработка-------------------------------------------------------
Set @j = 1;

While @j <= @jj

	BEGIN

	SELECT @sql_text = _query_text FROM @counter WHERE _id = @j 

	CREATE table #tables_names(attr_name nvarchar(max),schemaname nvarchar(max));
	INSERT INTO #tables_names exec sp_executesql @sql_text

	DECLARE  all_docs CURSOR FOR Select * From #tables_names

	OPEN all_docs
	FETCH NEXT FROM all_docs INTO @tablename,@schema_name
	set @K=0;
	WHILE @@FETCH_STATUS =0
	BEGIN
		-----------------------------------------------------------------------------------------------------------
		CREATE table #tables_attr(attr_name nvarchar(max),attr_prec nvarchar(max),attr_scale nvarchar(max));

		INSERT INTO #tables_attr
		exec sp_executesql @sql_query_text_doc,N'@tablename nvarchar(150)',@tablename;

		SELECT @sql_query_text = REPLACE(@sql_query_text_Reference,N'@tablename',QUOTENAME(@DBName)+N'.'+QUOTENAME(@schema_name)+N'.'+QUOTENAME(@tablename))+char(13);

		DECLARE  doc_numeric Cursor for Select * FROM #tables_attr

		OPEN doc_numeric; 
		FETCH NEXT FROM doc_numeric INTO @attr_name,@attr_prec,@attr_scale
		
		SET @i =0;
		WHILE @@FETCH_STATUS =0
		BEGIN
		-----------------------------------------------------------------------------------------------------------

			IF NOT @i=0
			SET @sql_query_text = @sql_query_text +','+char(13);	

			SET @sql_query_text = @sql_query_text +@attr_name +'=cast(rand(checksum(newid()))*0.75*'+@attr_name+' as Numeric('+@attr_prec+','+@attr_scale+'))'
			
			SET @i =@i+1;
			FETCH NEXT FROM doc_numeric INTO @attr_name,@attr_prec,@attr_scale
		-----------------------------------------------------------------------------------------------------------
		END;

		IF NOT @i = 0
		BEGIN
		PRINT @sql_query_text; 
		Exec(@sql_query_text);
		END;

		DROP TABLE #tables_attr

		CLOSE doc_numeric; 
		DEALLOCATE doc_numeric;
		-----------------------------------------------------------------------------------------------------------
		FETCH NEXT FROM all_docs INTO @tablename,@schema_name
		set @K = @K+1
	END;

	DROP TABLE #tables_names;
	CLOSE all_docs; 
	DEALLOCATE all_docs;

	set @j=@j+1;

END;
--------------------------------------------------------------------------------------------------------------------------------------------
--
END