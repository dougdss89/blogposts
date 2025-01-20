USE TSQLV6;
GO
create schema tmp;
go
set nocount on;


--img01
if object_id ('tmp.myorders') is not null drop table tmp.myorders;
go

select orderid, 
		custid, 
		cast(empid as bigint) as empid, 
		orderdate, 
		shipperid

into tmp.myorders
from sales.orders;
go

select top(5) * from tmp.myorders;
GO

--img02
-- excluindo a propriedade identity da coluna
if OBJECT_ID('tmp.myordersid') is not null drop table tmp.myordersid
go
select 
	isnull(orderid, 0) as tmporderid, -- isnull ou coalesce retiram o identity
	empid, 
	orderdate, 
	shipperid
into tmp.myordersid
from Sales.Orders;
go

select 
	object_id,
	[name] as colname,
	is_identity
from sys.all_columns where [name] = 'tmporderid';
go

select * from tmp.myordersid;
go

select * from tmp.myordersid;
go

drop table if exists tmp.colidt;
go
-- criando um coluna com identity

select 

    IDENTITY(int, 1,1) as newcol,
    empid, 
    orderdate,
    shipperid

into tmp.colidt
from sales.orders;
go

drop table if exists tmp.ordercol;
go

select top(100)

	IDENTITY(int, 1,1) as ordercolid,
	empid,
	orderdate
into tmp.ordercol
from Sales.Orders
order by ordercolid asc;
go

select * from tmp.ordercol;
go

if object_id('tmp.colidt') is not null drop table tmp.colidt;
go
-- se for necess�rio a ordem

create table tmp.colidt (
    colid int IDENTITY(1,1) not null,
    empid int,
    orderdate date,
    shipperid int);
GO
insert tmp.colidt (empid, orderdate, shipperid)
select empid, orderdate, shipperid
from sales.Orders;

select * from tmp.colidt;

--img05
-- o problema do select into � o lock na tabela
-- ele n�o bloqueia s� a tabela, mas os schemas.
if object_id('tmp.myorders') is not null drop table tmp.myorders;
go

begin tran
select orderid, custid, empid, shipperid, orderdate
into tmp.myorders
from Sales.orders;



--------------- DATA MANIPULATION: DELETE, TRUNCATE & UPDATE ---------------

drop table if exists tmp.truncatetb;
go

select 
    empid,
    custid,
    orderdate
into tmp.truncatetb
from sales.orders;
go

select * from tmp.truncatetb;
go

drop table if exists #temp_truncate;
go

select 
	empid,
	custid,
	orderdate
into #temp_truncate
from Sales.Orders;
go

select * from #temp_truncate;
go

truncate table #temp_truncate;
go

begin tran
truncate table tmp.truncatetb

rollback;
go
select * from tmp.truncatetb;
go

-- cria dados duplicados
-- criei a tabela tmp.duplicate com select into
drop table if exists tmp.duplicate;

insert into tmp.duplicate
select orderdate, custid, coalesce(orderid, 0) as orderid, shipcountry
from Sales.Orders;
go 5

select * from tmp.duplicate where orderid = 10248;

-- limpa duplicate tmp.duplicate
-- cria a cte para pr� filtrar  as duplicatas criando um numero para as linhas

with dedup_duplicate as (

	select
		orderdate, custid,
		orderid, shipcountry,
		ROW_NUMBER() over (partition by orderdate, custid, orderid, shipcountry
							order by orderid) as rn_dedup
	from tmp.duplicate)

delete dedup_duplicate
output
	deleted.orderdate,
	deleted.custid
where rn_dedup > 1

-- criando tabela de stage com select into

drop table if exists tmp.stagedup;
go

select *,
ROW_NUMBER() over (partition by orderdate, custid, orderid, shipcountry
					order by orderid, custid) as rn_dup
into tmp.stagedup
from tmp.duplicate;
go

select * from tmp.stagedup where orderid = 10248;

-- deletando duplicata da stage
begin tran
delete tmp.stagedup
output
	deleted.orderdate,
	deleted.custid,
	deleted.rn_dup
where rn_dup > 1;

commit;

select * from tmp.stagedup;

truncate table tmp.duplicate;
go

insert into tmp.duplicate
select orderdate, custid, orderid, shipcountry
from tmp.stagedup;
go

select * from tmp.duplicate;
go
begin tran

begin tran
delete from tmp.duplicate
output
    deleted.orderdate,
    deleted.custid,
    deleted.empid
where orderdate > '20200101' and orderdate <= '20201231'

rollback;

select * from tmp.truncatetb
where orderdate  <= '20201231'
go
         
-- deletando linhas duplicadas.
drop table if exists tmp.duptable;
go

select orderdate, orderid, empid, custid, freight
into tmp.duptable
from Sales.Orders
    cross join
        dbo.Nums
where n <= 10000;
go

select count(*) from tmp.duptable
where orderid = 10248
go

-- delete mesmo aplicado me uma CTE deleta na tabela fonte.
-- como a cte � apenas uma 'virtualiza��o' da tabela original, o comando acaba atuando na original.
-- por outro lado, abrir begin tran com cte acusa erro
begin tran
with delcte as (

    select orderdate, orderid
    from tmp.duptable
    where orderdate <= '20201231'
)

delete delcte
output
    deleted. orderdate,
    deleted.orderid


-- delete com output
begin TRAN
delete from tmp.duptable
output
    deleted.custid,
    deleted.empid
where orderdate <= '20201231'
rollback;

-- por�m, CTE � �til quando utilizada para deletar duplicatas

with deldup as (

    select *, 
    ROW_NUMBER() over(partition by orderid
                        order by orderid
                        ) as rnw
    from tmp.duptable
)

delete from deldup
output
deleted.orderdate,
deleted.orderid,
deleted.empid,
deleted.custid,
deleted.freight
where rnw <> 1

select * from duptable;

with dropdup as (

    select *,
        row_number() over(partition by orderid
                            order by (select null)) as rnw -- ordem arbitr�ria, n�o importa a ordem do delete)
    from tmp.duptable
)
delete from dropdup
output
deleted.*
where rnw > 1;
go

begin tran
truncate table tmp.duptable

rollback;

-- criar carga de dados duplicados
-- cada zero adicionado multiplicar por 830 --> 830 x 1000

drop table if exists tmp.batchdelete;
go
select 
	empid,
	custid,
	orderdate,
	shipcountry
into tmp.batchdelete
from sales.orders
	cross join
	dbo.nums
where n <= 10000;
go

with into_nodup as (

	select 
		empid, custid,
		orderdate, shipcountry,
		row_number() over (partition by empid, custid, orderdate
							order by custid, empid) as dup_rnw
	from tmp.batchdelete
)

select empid, custid, orderdate, shipcountry
into nodup_stg
from into_nodup
where dup_rnw = 1;
go

select * from nodup_stg;

-- truncate batchdelete

truncate table tmp.batchdelete;

-- carga batchdelete

insert into tmp.batchdelete
select * from nodup_stg;

select * from tmp.batchdelete;

-- deletando com looping em lotes de linhas
begin tran
while 1=1
	begin
			with batchdel as(

			select *, 
				row_number() over (partition by empid, custid, 
												orderdate, shipcountry
									order by custid) as batch_rwn
			from tmp.batchdelete
			)
		delete top(10000)
		from batchdel
		output
			deleted.empid,
			deleted.custid,
			deleted.batch_rwn
		where batch_rwn > 1

		if @@rowcount < 10000 break;
	end

rollback;

select * from tmp.batchdelete;
go

-- delete com looping de contagem de linhas
while 1 = 1 -- cria um loop infinito
BEGIN
with countdelete as (

    select *,
        row_number() over (partition by orderid
                            order by (select null)) as rnw -- ordem arbitr�ria
    from tmp.duptable
)

delete top(5000) from countdelete
where rnw <> 1;

if @@ROWCOUNT < 5000 break;
end

select * from tmp.duptable;
go


-- novo teste com remo��o de duplicatas

-- criando dados duplicados

select 
    empid, custid, orderdate, orderid
into tmp.newduplicate
from Sales.Orders
cross join
    dbo.Nums
where n <= 20000

select count(*) from tmp.newduplicate

select *
into tempdup
from tmp.newduplicate
where ROW_NUMBER() over (partition by orderid
                            order by (select null)) <> 1;
GO
-- como as WF s�o executadas durante o SELECT, ela precisa ser pr�-computada com uma CTE ou TEMP
select * from tempdup;
go

select *,
    row_number() over (partition by orderid
                        order by (select null)) as rnw
into #tbtempdup
from tmp.newduplicate

begin tran
delete from #tbtempdup
output
    deleted.custid,
    deleted.orderid,
    deleted.rnw
where rnw <> 1;
ROLLBACK

with stagedup as (

    select *,
        row_number() over (partition by orderid) as rwn
    from tmp.newduplicate
)
select * 
into tmp.salesnodup
from stagedup
where rwn = 1;
GO

---------- UPDATE E MERGE ----------

-- cria tabela um de stage para compara��o
drop table if exists tmp.stgcust;
go

select 
	identity(int, 1, 1) as stgkey,
	coalesce(custid, 0) as custid,
	city
into tmp.stgcust
from sales.customers
	cross apply
	dbo.nums 
where n < 100;
go

drop table if exists tmp.srccust;
go

select 
	identity(int, 1, 1) as stgkey,
	coalesce(custid, 0) as custid,
	city
into tmp.srccust
from sales.customers
	cross apply
	dbo.nums 
where n < 100;

-- modificiando a tabela tmp.stgcust

update tmp.stgcust
set city = 'not av'
output
	deleted.*,
	inserted.*
go

begin tran
	begin
		with join_upd as (

			select
				st.stgkey as custkey,
				st.custid as stcustid,
				st.city as stcity,
				sr.stgkey,
				sr.custid,
				sr.city as srcity
			from tmp.stgcust as st
				inner join
				tmp.srccust as sr
			on st.stgkey = sr.stgkey)

		update join_upd
		set stcity= srcity
		output
			deleted.stcity,
			inserted.stcity
		where stcity = 'not av'
	end;

rollback;


begin tran

	update tmp.stgcust
	set city = ps.city
	output
		deleted.city,
		inserted.city
	from tmp.stgcust as st
		inner join
		Production.suppliers as ps
	on st.custid = ps.supplierid
	and st.city <> ps.city;

commit;

select * from tmp.stgcust;



begin tran
	begin

		-- inserir valores reais em uma tabela tempor�ria	
		drop table if exists #tblookup;

		select distinct
			custid,
			city
		into #tblookup
		from Sales.customers;

		-- atualiza tabela
		update tmp.stgcust
		set city = tbl.city
		output
			deleted.city,
			inserted.city
		from tmp.stgcust as st
			inner join
			#tblookup as tbl
		on st.custid = tbl.custid
		and st.city <> tbl.city

	end
	drop table #tblookup;

commit;

-- UPATE COM OFF-SET FETCH
-- prepara os dados com CTE

begin tran
	begin
		with ct_offset as (
	
			select
				custid,
				empid,
				orderdate
			from tmp.batchdelete
			where orderdate <= '2020-12-01'
			/*order by orderdate
			offset 10 rows fetch next 30 rows only*/)

			update ct_offset
			set empid = 0

			print 'Row count is: ' + cast(@@rowcount as varchar(10)); --captura a quantidade de linha
	
	end;

rollback;
go

begin tran
while 1 = 1
	begin
		
		with upd_batch as (

			select
				empid,
				shipcountry
			from tmp.batchdelete)

	update top(5000) upd_batch
	set shipcountry = 'NA';

if @@ROWCOUNT < 5000 break;
end

rollback;
				
				
----- MERGE -----

-- cria tabelas de teste

drop table if exists tmp.mergestg;
go

select 
	coalesce(custid, 0) as custid,
	city
into tmp.mergestg
from Sales.customers
	cross apply
	dbo.Nums 
where n < 10000;

drop table if exists tmp.mergenew;
go

select 
	coalesce(custid,0) as custid,
	city
into tmp.mergenew
from Sales.customers
	cross apply
		dbo.nums
where n < 100000;
go

update tmp.mergenew
set companyname = 'Na'
where city not in ('Portland', 'Warszawa', 'Seattle', 'Montr�al', 'Paris', 'Lyon');

------ MERGE

--merge b�sico, deletando quando os ids forem iguais.
begin tran
	
	merge into tmp.mergenew as mgn
	using tmp.mergestg as mtg
	on mgn.custid = mtg.custid

	when matched then 
	delete
	
	output
		deleted.*;

rollback;

-- adicionando a coluna de id para ajustar o erro no comando merge.
alter table tmp.mergenew
add mrgid int identity

alter table tmp.mergestg
add mrgid int identity

-- atualizando tabela
begin tran

	merge into tmp.mergenew as mgn -- tabela alvo
	using tmp.mergestg as mtg -- tabela fonte
	on mgn.custid = mtg.custid
	

	when matched then
	update set
	mgn.custid = mtg.custid,
	mgn.companyname = mtg.companyname,
	mgn.city = mtg.city

	output
		deleted.custid,
		deleted.companyname,
		inserted.custid as new_id,
		inserted.companyname as newcompany;

rollback;

-- corrigindo o erro anterior
begin tran
	begin

		merge into tmp.mergenew as mgn
		using tmp.mergestg as mtg
		on mgn.mrgid = mtg.mrgid

		when matched and
		mgn.companyname <> mtg.companyname
		and mgn.companyname = 'na' 

		then

		update set
		mgn.companyname = mtg.companyname

		output
			deleted.companyname,
			inserted.companyname as newcompany;
	end;

rollback;

-- adicionando colunas de flag
-- merge com base em flag

alter table tmp.mergestg
add flagupd varchar(5) null;
go

alter table tmp.mergenew
add flagupd varchar(5) null;
go

update tmp.mergestg
set flagupd = 'no';

update tmp.mergenew
set flagupd = 'no';

--- atualizando uma porcentagem das linhas 

begin tran
	begin
		with percentupd as (

			select 
				top (40) percent flagupd
			from tmp.mergenew)

		update percentupd
		set flagupd = 'yes'
		output
			deleted.*,
			inserted.*
	end;

commit;

-- com a porcentagem atualizada, aplico o merge com base em um campo

begin tran

	merge into tmp.mergenew as mgn
	using tmp.mergestg as mgt
	on mgn.mrgid = mgt.mrgid

	when matched and mgn.flagupd = 'no'
	then

	update set
		mgn.flagupd = mgt.flagupd
	output 
		deleted.custid as delid,
		deleted.mrgid as delmgr,
		deleted.flagupd as delflag,
		inserted.custid,
		inserted.mrgid,
		inserted.flagupd;

rollback;
go

select * from tmp.mergenew
where flagupd = 'no';

-- inserindo com merge

begin tran
	
	merge tmp.mergenew as mgn
	using tmp.mergestg as mtg
	on mgn.mrgid = mtg.mrgid

	when matched and mgn.flagupd = 'no'
	then

	update set 
		mgn.flagupd = mtg.flagupd

	when not matched then
	insert (custid, city, companyname, flagupd)
	values (mtg.custid, mtg.city, mtg.companyname, mtg.flagupd)
	
	output
		deleted.custid as delcust,  deleted.city as delcity, 
		deleted.companyname as delcorp, deleted.mrgid as delid, 
		deleted.flagupd as delflag,

		inserted.custid, inserted.city, inserted.companyname,
		inserted.mrgid, inserted.flagupd;

rollback;

select * from tmp.mergenew
order by NEWID();

select top(100) * from tmp.mergestg 
order by NEWID();

-- adicionando colunas de tempo
alter table tmp.mergestg
add validfrom datetime not null
constraint dft_getdate default sysdatetime(),

validto datetime not null
constraint valid_date default '9999-12-31 00:00:00',

lineversion tinyint not null
constraint dft_version default 1;
go

-- criando scd-2 com merge

-- update mergestage

begin tran
	begin
		with updmergestg as (

			select top(40) percent flagupd
			from tmp.mergestg
			order by NEWID()
			) -- fim cte

			update updmergestg
			set flagupd = 'yes'

	end;

commit


/*
	primeiro forma a tabela para atualizar, para facilitar
	a l�gica e ter apenas as colunas necess�rias.

	procure limitar ao m�ximo o que deve ser atualizado para evitar problemas
*/

drop table if exists #tmpscd_table;
go

create table #tmpscd_table (

	custid int,
	city varchar(50),
	validfrom datetime,
	validto datetime,
	updated char(10),
	lineversion tinyint)
go

begin tran
declare 
	@uptime datetime,
	@updated char(10);

set @uptime = SYSDATETIME()
set @updated = 'updated';

	begin
		
		with join_update as (

			select 

				mtg.custid,
				mtg.city,
				mtg.mrgid,
				mtg.flagupd,
				mtg.lineversion,
				mtg.validfrom,
				mtg.validto,
				mgn.city as citynew,
				mgn.custid as custidnew,
				mgn.mrgid as mrgidnew,
				mgn.validto as newvalidto,
				mgn.validfrom as newvalidfrom

			from tmp.mergestg as mtg
				inner join
				tmp.mergenew as mgn
			on mtg.mrgid = mgn.mrgid
			and mtg.custid = mgn.custid
			where mtg.flagupd = 'yes')

			update join_update
			set
				custid = custidnew,
				city = citynew,
				validfrom = newvalidfrom,
				validto = '9999-12-31',
				flagupd = 'no',
				lineversion += 1
			output
				deleted.custid, deleted.city,
				deleted.validfrom, deleted.validto,
				deleted.flagupd, deleted.lineversion
			into #tmpscd_table

			insert into tmp.mergestg (custid, city, validfrom, validto, flagupd, lineversion)
			select custid, city, validfrom, @uptime, @updated, lineversion
			from #tmpscd_table
	end
rollback;

select * from #tmpscd_table;
go

select * from tmp.mergestg
where custid >= 5 and custid <= 10
and custid not in (9)
order by custid, lineversion;
go

alter table tmp.mergestg
alter column flagupd varchar(10) null;


-- SCD 2 COM MERGE
-- poderia colocar dentro do begin tran, a tabela tempor�ria

begin tran
	begin	
		declare 
			@uptime datetime,
			@updated char(10);

		set @uptime = SYSDATETIME()
		set @updated = 'updated';

		merge tmp.mergestg as stg
		using tmp.mergenew as mgn
		on stg.mrgid = mgn.mrgid
		and stg.custid = mgn.custid

		when matched and stg.flagupd = 'yes' 
		then

		-- atualiza para nova vers�o
		update set
		stg.custid = mgn.custid,
		stg.city = mgn.city,
		stg.flagupd = mgn.flagupd,
		stg.validfrom = mgn.validfrom,
		stg.validto = '9999-12-31',
		stg.lineversion += 1

		-- captura os valores deletados e insere na tabela tempor�ria
		output
			deleted.custid,
			deleted.city,
			deleted.validfrom,
			deleted.validto,
			deleted.flagupd,
			deleted.lineversion
		into #tmpscd_table;

		-- insere a partir da tabela tempor�ria, os valores na tabela original
		insert into tmp.mergestg (custid, city, validfrom, validto, flagupd, lineversion)
		select custid, city, validfrom, @uptime, @updated, lineversion
		from #tmpscd_table
end
rollback;


select * from tmp.mergestg
where custid = 8
order by lineversion;

select * from #tmpscd_table;

--SCD III - quando controlar apenas uma coluna
-- utilizando merge

alter table tmp.mergestg
add updatedcity varchar(15)
go

alter table tmp.mergestg
drop column validto;
go

exec sp_rename 'tmp.mergestg.validfrom', 'updated_date', 'column';

select * from tmp.mergestg;
go


drop table if exists #tmpscd_table;
		
create table #tmpscd_table (

	custid int,
	city nvarchar(50)
);

begin tran 
	begin
		declare @updaytime as datetime;
		set @updaytime = SYSDATETIME();

-- utilizando merge para controle
	
		merge into tmp.mergestg as stg
		using tmp.mergenew as mgn
		on stg.custid = mgn.custid

		when matched and stg.flagupd = 'yes'
		then

		update set

			stg.custid = mgn.custid,
			stg.updatedcity = mgn.city,
			stg.updated_date = @updaytime, -- marco o hor�rio da atualiza��o
			stg.flagupd = 'updated' -- indicando que a linha foi atualizada.

		output
			deleted.custid,
			deleted.city
		into #tmpscd_table;
	end
rollback;

select 
	custid,
	city,
	updatedcity as city_atual,
	flagupd,
	updated_date
from tmp.mergestg
order by flagupd desc;
go


-- criando SCD IV (controle hist�rico com tabela auxiliar)
drop table if exists tmp.stageproduct;
select 
	coalesce(productid,0) as productid,
	productname,
	categoryid,
	unitprice
into tmp.stageproduct
from Production.Products;
go

drop table if exists tmp.finalproduct;
select 
	coalesce(productid,0) as productid,
	productname,
	categoryid,
	unitprice
into tmp.finalproduct
from Production.Products;
go

-- atualizando linha para o update randomicamente
alter table tmp.stageproduct
add updflag varchar(5);
go

alter table tmp.finalproduct
add isupdated bit;
go

update tmp.finalproduct
set isupdated = 0;

update tmp.stageproduct
set updflag = 'no';

begin tran
	begin
		with upd_prodpercent as (

			select top(20) percent

				unitprice,
				updflag
			from tmp.stageproduct
			order by NEWID())

		update upd_prodpercent
		set 
			updflag = 'yes',
			unitprice = (unitprice *+ 1.20)
		output
			deleted.unitprice,
			inserted.unitprice as newprice,
			deleted.updflag,
			deleted.updflag as newflag
		where updflag = 'no'
	end
commit;

-- criando a tabela de registro hist�rico
create table tmp.product_hist (

	hist_prodid int,
	hist_prodname varchar(40),
	hist_categoryid int,
	hist_price decimal(12,2),
	row_version smallint,
	dateupdated datetime)
go

-- merge para controle

begin tran
	declare @dateupd as datetime, 
			@version as smallint;

	set		@dateupd = SYSDATETIME();
	set		@version = 0;

	begin 
		drop table if exists #tempprod;
		
		create table #tempprod (
				hist_prodid int,
				hist_prodname varchar(40),
				hist_categoryid int,
				hist_price decimal(12,2))
	end

	begin
		merge into tmp.finalproduct as fnp
		using tmp.stageproduct as stp
		on fnp.productid = stp.productid

		when matched and
		fnp.unitprice <> stp.unitprice
		or stp.updflag = 'yes'

		then

		update set

			fnp.productid = stp.productid,
			fnp.productname = stp.productname,
			fnp.categoryid = stp.categoryid,
			fnp.unitprice = stp.unitprice,
			fnp.isupdated = 1

		output
			deleted.productid,
			deleted.productname,
			deleted.categoryid,
			deleted.unitprice
		into #tempprod;

		begin tran
			begin
			-- atualizando o versionamento da linha na tabela hist�rica.

				insert into  tmp.product_hist (hist_prodid, hist_prodname, hist_categoryid, hist_price, row_version, dateupdated)
				select hist_prodid, hist_prodname, hist_categoryid, hist_price, @version + 1, @dateupd
				from #tempprod;
			end

			begin
				update  tmp.stageproduct
				set updflag = 'no'
				output
					deleted.updflag
				where updflag = 'yes';
			end
		commit;
	end
rollback; -- commit ou rollback para finalizar o merge.

select * from tmp.product_hist;

select * from tmp.finalproduct;

select
	productid,
	unitprice,
	hist_price,
	isupdated,
	row_version,
	dateupdated
from tmp.finalproduct as fnp
	inner join
	tmp.product_hist as hist
on fnp.productid = hist.hist_prodid



-- exemplo de erro utilizando ON como filtro.
merge into tmp.mergenew as mgn
using tmp.mergestg as stg
on mgn.custid = stg.custid 
and mgn.custid = 100

when matched then
update

when not matched then
delete;
