SET NOCOUNT ON
GO

IF EXISTS(select * from sysobjects where id = object_id(N'[dbo].[customer]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
BEGIN
 drop table [dbo].[customer]
END
GO

CREATE TABLE [dbo].[customer](
	[idcustomer] [int] NOT NULL,
	[idcustomerkind] [int] NOT NULL,
	[name] [varchar](100) NULL,
	[age] [int] NULL,
	[birth] [datetime] NULL,
	[surname] [varchar](100) NULL,
	[stamp] [datetime] NULL,
	random [int] NULL,
	curr decimal(19,2) NULL,
	cat int null,
	cat20 int null,
 CONSTRAINT [PK_customer] PRIMARY KEY CLUSTERED ([idcustomer] ASC ) ON [PRIMARY]
) ON [PRIMARY]

GO
declare @i int
set @i=1
select RAND(100)
while (@i<500) BEGIN
insert into customer(idcustomer,idcustomerkind,name,age,birth,surname,stamp,random,curr,cat,cat20) values(
			 @i,
			 (@i/10)+1,
			 'name'+convert(varchar(10),@i)
			,10+@i,
			{ts '2010-09-24 12:27:38.030'},
			'surname_'++convert(varchar(10),@i*2+100000),
			getdate(),
			RAND()*1000,
			RAND()*10000,
			@i/5,
			@i/20 +1
		)
set @i=@i+1
end
GO

IF EXISTS(select * from sysobjects where id = object_id(N'[dbo].[customerphone]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
BEGIN
 drop table [dbo].[customerphone]
END
GO

CREATE TABLE [dbo].[customerphone](
	[idcustomer] [int] NOT NULL,
	[idphone] [int] NOT NULL,
	[phonekind] [varchar](100) NULL,
	tel [varchar](40) NOT NULL
 CONSTRAINT [PK_customerphone] PRIMARY KEY CLUSTERED ([idcustomer] ASC,[idphone] ASC ) ON [PRIMARY]
) ON [PRIMARY]

GO
declare @i int
declare @j int
set @i=1
select RAND(100)
while (@i<500) BEGIN

set @j=1
while (@j<4) BEGIN
 insert into customerphone(idcustomer,idphone,phonekind,tel) values(
			 @i,
			 @j,
			 'phonename'+convert(varchar(2),@j),
			'tel_'+convert(varchar(10),@i)+'000'+convert(varchar(10),@j)
		)
	set @j=@j+1
END
set @i=@i+1
end
GO


IF EXISTS(select * from sysobjects where id = object_id(N'[dbo].[seller]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
BEGIN
 drop table [dbo].[seller]
END
GO
CREATE TABLE [dbo].[seller](
	[idseller] [int] NOT NULL,
	[idsellerkind] [int] NOT NULL,
	[name] [varchar](100) NULL,
	[age] [int] NULL,
	[birth] [datetime] NULL,
	[surname] [varchar](100) NULL,
	[stamp] [datetime] NULL,
	random [int] NULL,
	curr decimal(19,2) NULL,
	cf varchar(200)
 CONSTRAINT [PK_seller] PRIMARY KEY CLUSTERED ([idseller] ASC ) ON [PRIMARY]
) ON [PRIMARY]
GO

IF EXISTS(select * from sysobjects where id = object_id(N'[dbo].[selleractivity]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
BEGIN
 drop table [dbo].[selleractivity]
END
GO
CREATE TABLE [dbo].[selleractivity](
	[idseller] [int] NOT NULL,
	[idactivity] [int] NOT NULL,
	[description] [varchar](100) NULL
 CONSTRAINT [PK_selleractivity] PRIMARY KEY CLUSTERED ([idseller] ASC,[idactivity] ASC ) ON [PRIMARY]
) ON [PRIMARY]
GO


declare @i int,
        @j int
set @i=1
select RAND(1000)
while (@i<500) BEGIN
insert into seller (idseller,idsellerkind,name,age,birth,surname,stamp,random,curr,cf) values(
			 @i,
			 (@i/10)+1,
			 'name'+convert(varchar(10),@i),
			10+@i,
			{ts '2010-09-24 12:27:38.030'},
			'surname_'++convert(varchar(10),@i*2+100000),
			getdate(),
			RAND()*1000,
			RAND()*10000,
			convert(varchar(20),RAND()*100000)
		)
set @j=1
while (@j<4) begin
    insert into selleractivity (idseller,idactivity,description) values (
            @i, @j, 'activity'+convert(varchar(10),@i)+'-'+convert(varchar(10),@j)
    )
    set @j=@j+1
end

set @i=@i+1
end

GO



IF EXISTS(select * from sysobjects where id = object_id(N'[dbo].[sellerkind]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
BEGIN
 drop table [dbo].[sellerkind]
END
GO
CREATE TABLE [dbo].[sellerkind](
	[idsellerkind] [int] NOT NULL,
	[name] [varchar](100) NULL,
	rnd [int] NULL,
 CONSTRAINT [PK_sellerkind] PRIMARY KEY CLUSTERED ([idsellerkind] ASC ) ON [PRIMARY]
) ON [PRIMARY]
GO

declare @i int
set @i=0
select RAND(1000)
while (@i<50) BEGIN
insert into sellerkind (idsellerkind,name,rnd) values(
			 @i,
			 'seller kind n.'+convert(varchar(10),@i),
			RAND()*1000
		)
set @i=@i+1
end

GO

IF EXISTS(select * from sysobjects where id = object_id(N'[dbo].[customerkind]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
BEGIN
 drop table [dbo].[customerkind]
END
GO
CREATE TABLE [dbo].[customerkind](
	[idcustomerkind] [int] NOT NULL,
	[name] [varchar](100) NULL,
	rnd [int] NULL,
 CONSTRAINT [PK_customerkind] PRIMARY KEY CLUSTERED ([idcustomerkind] ASC ) ON [PRIMARY]
) ON [PRIMARY]
GO

declare @i int
set @i=1
select RAND(1000)
while (@i<=50) BEGIN
insert into customerkind (idcustomerkind,name,rnd) values(
			 @i,
			 'custom.kind-'+convert(varchar(10),@i),
			RAND()*1000
		)
set @i=@i+1
end

GO



if exists (select * from dbo.sysobjects where id = object_id(N'[testSP2]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [testSP2]
GO

 CREATE PROCEDURE testSP2
         @esercizio int,   @meseinizio int,   @mess varchar(200),   @defparam decimal(19,2) =  2
         AS
         BEGIN
         select 'aa' as colA, 'bb' as colB, 12 as colC , @esercizio as original_esercizio,
         replace(@mess,'a','z') as newmess,   @defparam*2 as newparam
         END

GO
if exists (select * from dbo.sysobjects where id = object_id(N'[testSP1]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [testSP1]
GO

CREATE PROCEDURE [dbo].[testSP1]
	@esercizio int,
	@meseinizio int,
	@mesefine int out,
	@mess varchar(200),
	@defparam decimal(19,2) =  2
AS
BEGIN
	set @mesefine= 12
	select 'a' as colA, 'b' as colB, 12 as colC , @esercizio as original_esercizio, 
		replace(@mess,'a','z') as newmess,
		@defparam*2 as newparam
END

GO

if exists (select * from dbo.sysobjects where id = object_id(N'[testSP3]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [testSP3]
GO
CREATE  PROCEDURE [dbo].[testSP3]
	@esercizio int = 0
AS
BEGIN
	select top 100 * from customer
	select top 100 * from seller
	select top 40 * from customerkind as c2
	select top 50 * from sellerkind as s2
END

GO

IF EXISTS(select * from sysobjects where id = object_id(N'[dbo].[sell]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
BEGIN
 drop table [dbo].[sell]
END
GO

CREATE TABLE [dbo].[sell](
    [idsell] [int] NOT NULL,
	[idcustomer] [int] NOT NULL,
	[idseller] [int] NOT NULL,
	[idcoseller] [int] NOT NULL,
	[idcoseller2] [int] NOT NULL,
	[idlist] [int]  NOT NULL,
	price decimal(19,2) NULL,
	place varchar(100) NULL,
	date smalldatetime  NULL
 CONSTRAINT [PK_sell] PRIMARY KEY CLUSTERED ([idsell] ASC ) ON [PRIMARY]
) ON [PRIMARY]
GO

IF EXISTS(select * from sysobjects where id = object_id(N'[dbo].[sellsupplement]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
BEGIN
 drop table [dbo].sellsupplement
END
GO

CREATE TABLE [dbo].[sellsupplement](
    [idsell] [int] NOT NULL,
    [idsupplement] [int] NOT NULL,
	[idselleraux] [int] NOT NULL,
	description varchar(100) NULL
 CONSTRAINT [PK_sellsupplement] PRIMARY KEY CLUSTERED ([idsell] ASC, [idsupplement] ASC ) ON [PRIMARY]
) ON [PRIMARY]
GO



declare @i int
declare @j int
set @i=1
select RAND(100)
while (@i<2000) BEGIN

set @j=1
while (@j<4) BEGIN
 insert into sell(	idsell,		idcustomer,		idseller,		idcoseller,		idcoseller2,	idlist,	price,		place) values(
					@i,			(@i % 20)+1,	(@i % 200)+1,	(@i % 200)+5,	(@i % 200)+6,	@j,		@j*100,'place_'+convert(varchar(10),@i)+'-'+convert(varchar(10),@j)
		)
 insert into sellsupplement(idsell, idsupplement,idselleraux,description) values (
                @i, (@i*10)+1, (@i%200)+40, 'supplement '+convert(varchar(10),@i)
 )
insert into sellsupplement(idsell, idsupplement,idselleraux,description) values (
                @i, (@i*10)+2, (@i%200)+40, 'supplement bis'+convert(varchar(10),@i)
 )

	set @j=@j+1
	set @i=@i+1
END

end
GO
--select * from customerphone where idcustomer=23

IF EXISTS(select * from sysobjects where id = object_id(N'[dbo].[customerview]') and OBJECTPROPERTY(id, N'IsView') = 1)
DROP VIEW [dbo].customerview
GO
IF EXISTS(select * from sysobjects where id = object_id(N'[dbo].[sellerview]') and OBJECTPROPERTY(id, N'IsView') = 1)
DROP VIEW [dbo].sellerview
GO


CREATE   VIEW [dbo].[customerview]
(
	idcustomer,
	idcustomerkind,
	customer,
	customerkind
)
as select
	C.idcustomer,
	C.idcustomerkind,
	C.name,
	CK.name
	from customer C
		left outer join customerkind CK on C.idcustomerkind= CK.idcustomerkind
GO

CREATE   VIEW [dbo].[sellerview]
(
	idseller,
	idsellerkind,
	seller,
	sellerkind
)
as select
	C.idseller,
	C.idsellerkind,
	C.name,
	CK.name
	from seller C
		left outer join sellerkind CK on C.idsellerkind= CK.idsellerkind
GO

IF EXISTS(select * from sysobjects where id = object_id(N'[dbo].[sellview]') and OBJECTPROPERTY(id, N'IsView') = 1)
DROP VIEW [dbo].sellview
GO


CREATE   VIEW [dbo].[sellview]
(
	idsell,
	place,
	idseller,
	idsellerkind,
	idcustomer,
	idcustomerkind,
	seller,
	sellerkind,
	customer,
	customerkind
)
as select
	S.idsell,
	S.place,
	S.idseller,
	SR.idsellerkind,
	S.idcustomer,
	C.idcustomerkind,
	SR.name,
	SRK.name,
	C.name,
	CK.name
	from sell S
		left outer join seller SR on S.idseller=SR.idseller
		left outer join sellerkind SRK on SR.idsellerkind= SRK.idsellerkind
		left outer join customer C on S.idcustomer =C.idcustomer
		left outer join customerkind CK on C.idcustomerkind= CK.idcustomerkind
GO

