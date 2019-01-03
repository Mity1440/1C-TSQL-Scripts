GO
SET NOCOUNT ON
--------ПОДГОТОВИТЕЛЬНЫЕ ДЕЙСТВИЯ---------------------------------------------------------------------------------------------------------------

declare @bdname		nvarchar(max) = N'Test_Base', 	-- Имя базы данных в MSSQL, в которой будем производить обрезку 
		@sql_query	nvarchar(max),
		@Erase_Date	nvarchar(max) = '20180101' 		-- Дата до которой будут удаляться данные
declare @metatables	TABLE (
[tableName] nvarchar(128),
[schname] nvarchar(128)
)

SELECT recovery_model_desc AS [Recovery Model],name FROM sys.databases where name =@bdname

if (SELECT recovery_model_desc AS [Recovery Model] FROM sys.databases where name = @bdname) ='FULL'
begin
	set @sql_query =N'ALTER DATABASE'+@bdname+N'SET RECOVERY SIMPLE'
	exec sp_executesql @sql_query
end

print 'Current DB Recovery model:'
SELECT recovery_model_desc AS [Recovery Model],name FROM sys.databases where name =@bdname

declare @tablename	nvarchar(128),
		@fulltablename nvarchar(128),
		@schname	nvarchar(128),
		@Offset		int,
		@Period		datetime,
		@r int,
		@Somecount	int
		
set @sql_query = 'SELECT @Offset = [Offset] FROM '+QUOTENAME(@bdname)+'.[dbo].[_YearOffset]'
exec sp_executesql @sql_query,N'@Offset int output', @Offset output
set @Period = DATEADD(Year, @Offset, @Erase_Date);  

print 'Erase date: '+convert (varchar(max),@Period,102)

--------ОЧИСТКА ТАБЛИЦ РЕГИСТРАЦИИ ИЗМЕНЕНИЙ---------------------------------------------------------------------------------------------------------------

set @sql_query = N'select sysT.Name as tablename ,schT.Name as schname FROM '+QUOTENAME(@bdname)+'.sys.Tables as sysT left join ' 
+QUOTENAME(@bdname)+'.sys.schemas as schT on sysT.schema_id = schT.schema_id where  CHARINDEX(''ChngR'', sysT.Name)<>0  order by sysT.Name'
insert into @metatables exec(@sql_query)
DECLARE db_cursor_сhng CURSOR FOR SELECT * FROM @metatables
OPEN db_cursor_сhng
FETCH NEXT FROM db_cursor_сhng INTO @tablename,@schname
WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @fulltablename = QUOTENAME(@bdname)+'.'+QUOTENAME(@schname)+'.'+QUOTENAME(@tablename)
		SET @sql_query = N'TRUNCATE TABLE '+@fulltablename;
		exec sp_executesql @sql_query;
		print 'Таблица ' + @tablename + ' очищена командой: '+ @sql_query
		FETCH NEXT FROM db_cursor_сhng INTO @tablename,@schname
	END
CLOSE db_cursor_сhng; 
DEALLOCATE db_cursor_сhng; 	SET NOCOUNT OFF
delete from @metatables;

--------ОЧИСТКА РЕГИСТРОВ СВЕДЕНИЙ---------------------------------------------------------------------------------------------------------------

set @sql_query = 
N'select [Tab].[name],[Sch].[Name] as [Table] from'+QUOTENAME(@bdname)+N'.sys.tables as [Tab]  
inner join'+QUOTENAME(@bdname)+N'.sys.columns as [Col] on [Tab].[object_id] = [Col].[object_id]  
inner join'+QUOTENAME(@bdname)+N'.sys.schemas as [Sch] on [Tab].[schema_id] = [Sch].[schema_id]  
where [Col].[Name] = ''_Period''  and ([Tab].[name] like ''%InfoRg[0-9]%'' or [Tab].[name] like ''%InfoRgSL%'' or [Tab].[name] like ''%InfoRgSF%'') 
order by [Tab].[name]'

insert into @metatables exec(@sql_query)

DECLARE db_cursor_inforg CURSOR FOR select * from @metatables
Open db_cursor_inforg;
FETCH NEXT FROM db_cursor_inforg INTO @tablename,@schname

WHILE @@FETCH_STATUS = 0
	BEGIN
		PRINT '-----------------------<Текущий обьект: '+@tablename+' >-----------------------'
		SET @fulltablename = QUOTENAME(@bdname)+'.'+QUOTENAME(@schname)+'.'+QUOTENAME(@tablename)
		SET @sql_query = N'(SELECT @Somecount= count(*) FROM '+QUOTENAME(@bdname)+N'.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '''+@tablename+''' and COLUMN_NAME= N''_Period'')'
		
		EXEC  sp_executesql @sql_query,N'@Somecount int output', @Somecount output

		IF @Somecount=1
		 BEGIN 

			DECLARE 
					@rowcount_in_period		int,
					@rowcount_total			int

			SET	 @sql_query=N'select @rowcount = COUNT_BIG(*) from '+@fulltablename+N' WITH(NOLOCK) WHERE _Period < '''+ convert (varchar(max) , @Period, 102 )+N'''';
			EXEC sp_executesql @sql_query,N'@rowcount int output', @rowcount_in_period output 
			
			SET @sql_query = N'SELECT @rowcount = ISNULL(SUM(row_count),0) FROM '+QUOTENAME(@bdname)+N'.sys.dm_db_partition_stats WHERE object_id=OBJECT_ID('''+ @fulltablename + N''') AND (index_id < 2)';
			EXEC sp_executesql @sql_query,N'@rowcount int output', @rowcount_total output;

			PRINT 'rowcount_in_period: ' +cast(@rowcount_in_period as varchar(max)) 
			PRINT 'rowcount_total: '	 +cast(@rowcount_total as varchar(max))
	
			IF @rowcount_in_period=@rowcount_total and @rowcount_in_period > 0 --если в выборку попадают все строки , то просто очищаю таблицу
				BEGIN 
					SET @sql_query = N'TRUNCATE TABLE ' + @fulltablename;
					EXEC sp_executesql @sql_query;
					PRINT 'Таблица ' + @fulltablename + ' очищена командой: '+ @sql_query
				END 
			ELSE IF @rowcount_in_period > 3000000
				BEGIN 
					SET @sql_query=N'SELECT * INTO #Holdingtable_0 FROM '+ @fulltablename +N' WHERE _Period > @Period'+
									   N' TRUNCATE table '+ @fulltablename+
									   N' INSERT into '+ @fulltablename +N' SELECT * FROM #Holdingtable_0'+
									   N' DROP table #Holdingtable_0'
					EXEC sp_executesql @sql_query,N'@Period datetime',@Period =@Period ;
					PRINT @sql_query
				END 
			ELSE IF @rowcount_in_period > 0
				BEGIN 
					SET @r = 1;
					WHILE @r > 0
						BEGIN
						SET @sql_query = N'DELETE t1 from (select top (500000) * FROM '+ @fulltablename +N' WHERE _Period < @Period order by _Period) t1 '
						EXEC sp_executesql @sql_query,N'@Period datetime',@Period =@Period ;
						SET @r = @@ROWCOUNT;
						print @r 
						END
					END print @sql_query
		 END 		
		ELSE
			BEGIN
				PRINT 'Обьект:'+@fulltablename+'не имеет поля _Period..'
			END 		
FETCH NEXT FROM db_cursor_inforg INTO @tablename,@schname
END;
CLOSE db_cursor_inforg; 
DEALLOCATE db_cursor_inforg;
delete from @metatables;

--------ОЧИСТКА РЕГИСТРОВ НАКОПЛЕНИЯ,БУХГАЛТЕРИИ---------------------------------------------------------------------------------------------------------------

set @sql_query = 
N'select [Tab].[name],[Sch].[Name] as [Table] from'+QUOTENAME(@bdname)+N'.sys.tables as [Tab]  
inner join'+QUOTENAME(@bdname)+N'.sys.schemas as [Sch] on [Tab].[schema_id] = [Sch].[schema_id]  
where ([Tab].[name] like ''%AccRg[0-9]%'' or [Tab].[name] like ''%AccRgAT%'' or [Tab].[name] like ''%AccRg[^A][A-W][0-9]''
or [Tab].[name] like ''%AccumRgT%'' or [Tab].[name] like ''%AccumRg[0-9]%'') 
order by [Tab].[name]'

insert into @metatables exec(@sql_query)

DECLARE db_cursor_acc CURSOR FOR select * from @metatables
Open db_cursor_acc;
FETCH NEXT FROM db_cursor_acc INTO @tablename,@schname

WHILE @@FETCH_STATUS = 0
	BEGIN

	print '-----------------------<Текущий обьект: '+@tablename+' >-----------------------'
	SET @fulltablename = QUOTENAME(@bdname)+'.'+QUOTENAME(@schname)+'.'+QUOTENAME(@tablename)

	DECLARE @rowcount_in_period_buh		int,
			@rowcount_total_buh			int

		SET	 @sql_query=N'select @rowcount = COUNT_BIG(*) from '+@fulltablename+N' WITH(NOLOCK) WHERE _Period < '''+ convert (varchar(max) , @Period, 102 )+N'''';
		EXEC sp_executesql @sql_query,N'@rowcount int output', @rowcount_in_period_buh output 
			
		SET @sql_query =  N'select @rowcount = COUNT_BIG(*) from ' + @fulltablename+N' WITH(NOLOCK)'
		EXEC sp_executesql @sql_query,N'@rowcount int output', @rowcount_total_buh output;

		PRINT 'rowcount_in_period_buh: ' +cast(@rowcount_in_period_buh as varchar(max)) 
		PRINT 'rowcount_total_buh: '	 +cast(@rowcount_total_buh as varchar(max))

		if @rowcount_in_period_buh=@rowcount_total_buh and @rowcount_in_period_buh > 0 --если в выборку попадают все строки , то просто очищаю таблицу
			BEGIN 
				SET @sql_query = N'TRUNCATE TABLE ' + @fulltablename;
				EXEC sp_executesql @sql_query;
				PRINT 'Таблица ' + @fulltablename + ' очищена командой: '+ @sql_query
			END 
		else if @rowcount_in_period_buh > 3000000
			BEGIN 
				SET @sql_query=N'SELECT * INTO #Holdingtable_0 FROM '+ @fulltablename +N' WHERE _Period > @Period'+
									N' TRUNCATE table '+ @fulltablename+
									N' INSERT into '+ @fulltablename +N' SELECT * FROM #Holdingtable_0'+
									N' DROP table #Holdingtable_0'
				EXEC sp_executesql @sql_query,N'@Period datetime',@Period =@Period ;
				PRINT @sql_query
			END 
		else if @rowcount_in_period_buh > 0
			begin 
				SET @r = 1;
				WHILE @r > 0
				BEGIN
					SET @sql_query=N'DELETE t1 from (select top (200000) * FROM '+ @fulltablename +N' WHERE _Period < @Period order by _Period) t1 '
					EXEC sp_executesql @sql_query,N'@Period datetime',@Period =@Period ;
					SET @r = @@ROWCOUNT;
				END print @sql_query
			END;
FETCH NEXT FROM db_cursor_acc INTO @tablename,@schname
	END
CLOSE db_cursor_acc; 
DEALLOCATE db_cursor_acc;
delete from @metatables;
-----------------------ДОКУМЕНТОВ И ТАБЛИЧНЫХ ЧАСТЕЙ---------------------------------------------------------------------------------------------------------------
DECLARE @name_vt nvarchar(128),
@fulltablenameVT nvarchar(128)

set @sql_query = 
N'select [Tab].[name] as [tableName],[Sch].[Name] as [schname] from'+QUOTENAME(@bdname)+N'.sys.tables as [Tab]  
inner join'+QUOTENAME(@bdname)+N'.sys.schemas as [Sch] on [Tab].[schema_id] = [Sch].[schema_id]  
where ([Tab].[name] like ''%Document%''
and SUBSTRING([Tab].[name], CHARINDEX(''_'', [Tab].[name])+1, Len([Tab].[name]) - CHARINDEX(''_'', [Tab].[name]))  not like ''%_VT%''
and SUBSTRING([Tab].[name], CHARINDEX(''_'', [Tab].[name])+1, Len([Tab].[name]) - CHARINDEX(''_'', [Tab].[name]))  not like ''%ChngR%''
and SUBSTRING([Tab].[name], CHARINDEX(''_'', [Tab].[name])+1, Len([Tab].[name]) - CHARINDEX(''_'', [Tab].[name]))  not like ''%Journal%'') 
order by [Tab].[name]'

insert into @metatables exec(@sql_query)

DECLARE db_cursor CURSOR FOR select * from @metatables
Open db_cursor;
FETCH NEXT FROM db_cursor INTO @tablename,@schname

WHILE @@FETCH_STATUS = 0 

	BEGIN 

		SET @fulltablename = QUOTENAME(@bdname)+'.'+QUOTENAME(@schname)+'.'+QUOTENAME(@tablename)
		-- табличные части документов
		CREATE table #tables_vt(Tname nvarchar(128));

		set @sql_query=N' SELECT name as Tname FROM sys.Tables where sys.Tables.name like ''%''+@n+''[_]%'' and not CHARINDEX(''VT'', name)=0'  
	
		Insert INTO #tables_vt 
		exec sp_executesql @sql_query,N'@n nvarchar(200)',@n=@tablename;
	
		Select Tname as 'Найденные обьекты' from #tables_vt 

		DECLARE db_cursor_vt CURSOR FOR select * FROM #tables_vt;
		OPEN db_cursor_vt
		FETCH NEXT FROM db_cursor_vt INTO @name_vt

		WHILE @@FETCH_STATUS = 0
			BEGIN

				print '-----------------------< Расширения документа : '+@name_vt+' >-----------------------'

				SET @fulltablenameVT = QUOTENAME(@bdname)+'.'+QUOTENAME(@schname)+'.'+QUOTENAME(@name_vt)

				DECLARE @rowcount_in_period_doc		int,
				@rowcount_total_doc			int

				SET	 @sql_query=N'select @rowcount = COUNT_BIG(*) FROM ' +@fulltablenameVT+ N' WITH(NOLOCK) WHERE '+@tablename+N'_IDRRef IN (SELECT _idRRef FROM ' + @fulltablename + N' as TableDocs Where [_Date_Time] <'+ ''''+Cast(@Period as nvarchar(36))+''')';
				exec sp_executesql @sql_query,N'@rowcount int output', @rowcount_in_period_doc output 
				---
				SET @sql_query = N'select @rowcount = COUNT_BIG(*) from ' + @fulltablenameVT+N' WITH(NOLOCK)';
				exec sp_executesql @sql_query,N'@rowcount int output', @rowcount_total_doc output;
	
				print '@rowcount_in_period_doc: '+ cast(@rowcount_in_period_doc as varchar(max)) 
				print '@rowcount_total_doc: '    + cast(@rowcount_total_doc as varchar(max))
		
				if @rowcount_in_period_doc = @rowcount_total_doc and @rowcount_in_period_doc > 0 --если в выборку попадают все строки , то просто очищаю таблицу
					begin 
						SET @sql_query = N'TRUNCATE TABLE ' + @fulltablenameVT;
						EXEC sp_executesql @sql_query;
						print 'Таблица ' + @fulltablenameVT + ' очищена командой: '+ @sql_query
					end
				else if @rowcount_in_period_doc > 3000000
					begin 
						SET @sql_query	  =N'SELECT * INTO #Holdingtable_2 FROM '+ @fulltablenameVT +N' WHERE ' + @tablename + N'_IDRRef NOT IN (SELECT _idRRef FROM ' +@fulltablename+ N' as TableDocs Where [_Date_Time] <'+ ''''+Cast(@Period as nvarchar(36))+''')'+
						N' TRUNCATE table '+ @fulltablenameVT +
						N' INSERT into '+ @fulltablenameVT +N' SELECT * FROM #Holdingtable_2'+
						N' DROP table #Holdingtable_2'
						print @sql_query
						EXEC sp_executesql @sql_query
					end 
				else if @rowcount_in_period_doc > 0
					begin 
						SET @r = 1;
						WHILE @r > 0
							BEGIN
								SET @sql_query = N'DELETE t1 from (select top (200000) * FROM '+ @fulltablenameVT +N' WHERE ' + @tablename + N'_IDRRef IN (SELECT _idRRef FROM ' +@fulltablename+ N' as TableDocs Where [_Date_Time] <'+ ''''+Cast(@Period as nvarchar(36))+''') ) t1 '
								EXEC(@sql_query);
								SET @r = @@ROWCOUNT;
							END Print @sql_query;
					end ;
				FETCH NEXT FROM db_cursor_vt INTO @name_vt
			end ;
		-------------------------------------------------
		--END

		DROP TABLE #tables_vt

		CLOSE db_cursor_vt; 
		DEALLOCATE db_cursor_vt; 

		-- очистка самих документов	
		print '-----------------------< Документ : '+@fulltablename+' >-----------------------'
		DECLARE @rowcount_in_period_document		int,
		@rowcount_total_document			int

		SET	 @sql_query=N'select @rowcount = COUNT_BIG(*) FROM ' +@fulltablename+ N' WITH(NOLOCK)  WHERE [_Date_Time] <'+ ''''+Cast(@Period as nvarchar(36))+'''';
		exec sp_executesql @sql_query,N'@rowcount int output', @rowcount_in_period_document output 
		
		SET @sql_query = N'SELECT @rowcount = ISNULL(SUM(row_count),0) FROM sys.dm_db_partition_stats WHERE object_id=OBJECT_ID('''+ @fulltablename + N''') AND (index_id<2)';
		exec sp_executesql @sql_query,N'@rowcount int output', @rowcount_total_document output;
	    
		print '@rowcount_in_period_document: ' + cast(@rowcount_in_period_document as varchar(max)) 
		print '@rowcount_total_document: '+		 cast(@rowcount_total_document as varchar(max)) 
		
		if @rowcount_in_period_document = @rowcount_total_document and @rowcount_in_period_document > 0  --если в выборку попадают все строки , то просто очищаю таблицу
			begin 
				SET @sql_query = N'TRUNCATE TABLE ' + @fulltablename;
				EXEC sp_executesql @sql_query;
				print 'Таблица ' + @fulltablename + ' очищена командой: '+ @sql_query
			end 
		else if @rowcount_in_period_document > 3000000
			begin 
				SET @sql_query =N'SELECT * INTO #Holdingtable_3 FROM '+ @fulltablename +N' WHERE [_Date_Time] >'+ ''''+Cast(@Period as nvarchar(36))+''''+N' order by _Date_Time'+
				N' truncate table '+ @fulltablename +
				N' INSERT into '+ @fulltablename +N' SELECT * FROM #Holdingtable_3'+
				N' DROP table #Holdingtable_3'
				print @sql_query
				EXEC sp_executesql @sql_query
			end 
		else if @rowcount_in_period_document > 0
			begin 
				SET @r = 1;
				WHILE @r > 0
					BEGIN
						SET @sql_query =N'DELETE t1 from (select top (200000) * FROM '+ @fulltablename +N' WHERE [_Date_Time] < '+ ''''+Cast(@Period as nvarchar(36))+''''+N' order by _Date_Time) t1 '
						Print @sql_query;
						EXEC(@sql_query);
						SET @r = @@ROWCOUNT;
					END 
			END;
		-----------------------------------
		FETCH NEXT FROM db_cursor INTO @tablename,@schname  
	END; 
CLOSE db_cursor  
DEALLOCATE db_cursor 
