drop table IF EXISTS customer;



CREATE TABLE customer(
	idcustomer int NOT NULL,
	idcustomerkind int NOT NULL,
	name varchar(100) NULL,
	age int NULL,
	birth datetime NULL,
	surname varchar(100) NULL,
	stamp datetime NULL,
	random int NULL,
	curr decimal(19,2) NULL,
	cat int null,
	cat20 int null,
    PRIMARY  KEY PK_customer (idcustomer)
);

GO


DROP PROCEDURE if exists ctemp;
GO


CREATE PROCEDURE ctemp ()
BEGIN
set @i=1;
while @i < 500 DO
insert into customer(idcustomer,idcustomerkind,name,age,birth,surname,stamp,random,curr,cat,cat20) values(
			 @i,  ROUND(cast(@i as DECIMAL(10,2))/10)+1,
			 concat('name',convert(@i,CHAR(10)) ),
			 10+@i,
			'2010-09-24 12:27:38',
			concat('surname_',convert(@i*2+100000, CHAR(10))),
			NOW(),
			RAND()*1000,
			RAND()*10000,
			ROUND(@i/5),
			ROUND(@i/20) +1
		);
set @i=@i+1;
END WHILE;
END

GO
call ctemp;
GO

drop table IF EXISTS customerphone;


CREATE TABLE customerphone(
	idcustomer int NOT NULL,
	idphone int not null,
	phonekind varchar(100) NULL,
	tel varchar(40) NULL,
    PRIMARY  KEY PK_customerphone (idcustomer,idphone)
);

GO
DROP PROCEDURE if exists ctemp;

CREATE PROCEDURE ctemp ()
BEGIN
set @i=1;
while @i < 500 DO
    set @j=1;
    while (@j<4) DO
    insert into customerphone(idcustomer,idphone,phonekind,tel) values(
			 @i,  @j,
			 concat('phonename',convert(@j,CHAR(2)) ),
			concat('tel_',convert(@i, CHAR(10)),'000',convert(@j, CHAR(10)))
		);
	set @j=@j+1;
	END WHILE;
    set @i=@i+1;
END WHILE;
END
GO

call ctemp;
GO
DROP PROCEDURE if exists ctemp;
GO



drop table IF EXISTS seller;


CREATE TABLE seller(
	idseller int NOT NULL,
	idsellerkind int NOT NULL,
	name varchar(100) NULL,
	age int NULL,
	birth datetime NULL,
	surname varchar(100) NULL,
	stamp datetime NULL,
	random int NULL,
	curr decimal(19,2) NULL,
	cf varchar(200),
	PRIMARY  KEY PK_seller (idseller)
);

GO


drop table IF EXISTS selleractivity;


CREATE TABLE selleractivity(
	idseller int NOT NULL,
	idactivity int NOT NULL,
	description  varchar(100) NULL,
	PRIMARY  KEY PK_selleractivity (idseller,idactivity)
);

GO


CREATE PROCEDURE ctemp ()
BEGIN
set @i=1;
while @i < 500 DO
    insert into seller(idseller,idsellerkind,name,age,birth,surname,stamp,random,curr,cf) values(
			 @i,  ROUND((@i-2)/10.0)+1,
			 concat('name',convert(@i,CHAR(10)) ),
			 10+@i,
			'2010-09-24 12:27:38',
			concat('surname',convert(@i*2+100000,CHAR(10)) ),
			now(),
			RAND()*1000,
            RAND()*10000,
            convert(RAND()*100000,CHAR(20))
		);
    set @j=1;
    while (@j<4) DO
         insert into selleractivity (idseller,idactivity,description) values (
                    @i, @j,concat('activity',convert(@i,CHAR(10)),'-',convert(@j,char(10)))
            );
    	set @j=@j+1;
	END WHILE;
    set @i=@i+1;
END WHILE;
END
GO


call ctemp;
GO
DROP PROCEDURE if exists ctemp;
GO


drop table IF EXISTS sellerkind;



CREATE TABLE sellerkind(
	idsellerkind int NOT NULL,
	name varchar(100) NULL,
	rnd int NULL,
    KEY PK_sellerkind (idsellerkind)
);



CREATE PROCEDURE ctemp ()
BEGIN
set @i=0;
while (@i<50) DO
insert into sellerkind (idsellerkind,name,rnd) values(
			 @i,
			 concat('seller kind n.',convert(@i,char(10))),
			 RAND()*1000
		);
set @i=@i+1;
end while;

END
GO
call ctemp;


DROP PROCEDURE if exists ctemp;


GO

drop table IF EXISTS customerkind;

CREATE TABLE customerkind(
	idcustomerkind int NOT NULL,
	name varchar(100) NULL,
	rnd int NULL,
     KEY PK_customerkind (idcustomerkind)
) ;

GO

CREATE PROCEDURE ctemp ()
BEGIN
set @i=0;
while (@i<50) DO
insert into customerkind (idcustomerkind,name,rnd) values(
			 @i,
			 concat('custom.kind-',convert(@i,char(10))),
			RAND()*1000
		);
set @i=@i+1;
end while;


END

GO

call ctemp;


DROP PROCEDURE if exists ctemp;




DROP PROCEDURE IF EXISTS testSP2;

GO



CREATE PROCEDURE testSP2 (IN esercizio int,   IN meseinizio int,   IN mess varchar(200),   IN defparam decimal(19,2) )
BEGIN
         if (defparam is null) THEN set defparam=2; 		 END IF;
         select 'aa' as colA, 'bb' as colB, 12 as colC , esercizio as original_esercizio,
         replace(mess,'a','z') as newmess,   defparam*2 as newparam;
END
GO



DROP PROCEDURE if exists testSP1;
GO

CREATE PROCEDURE testSP1( esercizio int, meseinizio int, out mesefine int ,	mess varchar(200), 	defparam decimal(19,2) )
BEGIN
	if (defparam is null) THEN set defparam=2; 		 END IF;
	set mesefine= 12;
	select 'a' as colA, 'b' as colB, 12 as colC , esercizio as original_esercizio,
		replace(mess,'a','z') as newmess,
		defparam*2 as newparam;
END
GO


DROP PROCEDURE IF EXISTS testSP3;

GO

CREATE  PROCEDURE  testSP3 (esercizio int)
BEGIN
    IF (esercizio IS NULL) then set esercizio=0; end IF;
	select * from customer limit 100;
	select * from seller limit 100;
	select * from customerkind as c2 limit 40;
	select * from sellerkind as s2 limit 50;
END

GO


drop table IF EXISTS sell;

CREATE TABLE sell(
	 idsell int NOT NULL,
    	idcustomer int NOT NULL,
    	idseller int NOT NULL,
    	idcoseller int NOT NULL,
    	idcoseller2 int NOT NULL,
    	idlist int  NOT NULL,
    	price decimal(19,2) NULL,
    	place varchar(100) NULL,
    	date date  NULL,
     PRIMARY KEY PK_sell (idsell)
) ;

GO

drop table IF EXISTS sellsupplement;

CREATE TABLE sellsupplement(
	 idsell int NOT NULL,
    	idsupplement int NOT NULL,
    	idselleraux int NOT NULL,
    	description varchar(100) NULL,
    PRIMARY KEY PK_sellsupplement (idsell,idsupplement)
);

GO


CREATE PROCEDURE ctemp ()
BEGIN
set @i=1;
while (@i<2000) DO

    set @j=1;
    while (@j<4) DO

     insert into sell(	idsell,		idcustomer,		idseller,		idcoseller,		idcoseller2,
                idlist,	price,		place) values(
                        @i,			(@i % 20)+1,	(@i % 200)+1,	(@i % 200)+5,	(@i % 200)+6,
                @j,		@j*100, concat('place_',convert(@i,char(10)),'-',convert(@j,char(10)))  );
            insert into sellsupplement(idsell, idsupplement,idselleraux,description) values (
                            @i, (@i*10)+1, (@i%200)+40, concat('supplement ',convert(@i,char(10)))  );
            insert into sellsupplement(idsell, idsupplement,idselleraux,description) values (
                            @i, (@i*10)+2, (@i%200)+40, concat('supplement bis',convert(@i,char(10))) );
            set @j=@j+1;
            set @i=@i+1;
     end while;
end while;

END

GO

call ctemp;


DROP PROCEDURE IF EXISTS ctemp;



DROP VIEW  IF EXISTS customerview;

CREATE   VIEW customerview
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

DROP VIEW  IF EXISTS sellerview;

CREATE   VIEW sellerview
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


DROP VIEW  IF EXISTS sellview;

CREATE   VIEW sellview
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
