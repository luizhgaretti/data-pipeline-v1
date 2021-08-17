/*
	PROJETO: PROJETO DE ESTUDO DE PIPELINE DE DADOS (https://luizhgaretti.com.br/01-arquitetura-projeto-data-pipeline/)
	SCRIPT: Script que cria a parte de banco de dados que servirá como origem para o Data Lake
	AZURE SQL DATABASE
	WWWW.DATAISBIG.COM.BR
	https://www.linkedin.com/in/luizhenriquegaretti/
*/

-- PARTE 01 > DDL
USE [bd-sql-loja]
GO

-- Tabela dos Produtos (Carros)
IF NOT EXISTS (SELECT * FROM sys.tables where [name] = 'tbProduct')
BEGIN
	CREATE TABLE dbo.tbProduct
	(
		idProduct					INT							NOT NULL IDENTITY PRIMARY KEY,
		NmProduct					VARCHAR(255)				NOT NULL,
		vlProduct					NUMERIC(10,2)				NOT NULL,
		dtLastUpdate				DATETIME DEFAULT GETDATE()	NOT NULL
	);
END
GO

-- Tabela das Lojas pelo Brasil
IF NOT EXISTS (SELECT * FROM sys.tables where [name] = 'tbStore')
BEGIN
	CREATE TABLE dbo.tbStore
	(
		idStore					INT								NOT NULL IDENTITY PRIMARY KEY,
		idCep					INT								NOT NULL,
		NmStore					VARCHAR(255)					NOT NULL,
		dtLastUpdate			DATETIME DEFAULT GETDATE()		NOT NULL
	);

	ALTER TABLE dbo.tbStore ADD CONSTRAINT fk_tbStore_tbCep FOREIGN KEY (idCep) REFERENCES tbCep(idCep);
END
GO

-- Tabela dos Clientes
IF NOT EXISTS (SELECT * FROM sys.tables where [name] = 'tbStore')
BEGIN
	CREATE TABLE dbo.tbCustomer
	(
		idCustomer		INT				NOT NULL IDENTITY PRIMARY KEY,
		idCep			INT				NOT NULL,
		nmCustomer		VARCHAR(250)	NOT NULL,
		dsCPF			VARCHAR(50)		NOT NULL,
		dsSexo			CHAR(1)			NOT NULL,
		dtLastUpdate	DATETIME		NOT NULL
	);

	ALTER TABLE dbo.tbStore ADD CONSTRAINT fk_tbCustomer_tbCep FOREIGN KEY (idCep) REFERENCES tbCep(idCep);
END
GO

-- Tabela de Vendas
IF NOT EXISTS (SELECT * FROM sys.tables where [name] = 'tbStore')
BEGIN
	CREATE TABLE dbo.tbSalesOrders
	(
		idSalesOrders			INT				NOT NULL IDENTITY PRIMARY KEY,
		idCustomer				INT				NOT NULL,	
		idStore					INT				NOT NULL,
		vlSalesOrders			NUMERIC(10,2)	NOT NULL,
		vlDiscountPercent		TINYINT			NULL,								
		dsStatusSalesOrders		VARCHAR(50)		NOT NULL,
		dtSalesOrders			DATETIME		NOT NULL
	)

	ALTER TABLE dbo.tbSalesOrders ADD CONSTRAINT fk_tbSalesOrders_tbCustomer FOREIGN KEY (idCustomer) REFERENCES tbCustomer(idCustomer);
	ALTER TABLE dbo.tbSalesOrders ADD CONSTRAINT fk_tbSalesOrders_tbStore FOREIGN KEY (idStore) REFERENCES tbStore(idStore);
END
GO

-- Tabela Item de Venda
IF NOT EXISTS (SELECT * FROM sys.tables where [name] = 'tbStore')
BEGIN
	CREATE TABLE dbo.tbItemSalesOrders
	(
		idItemSalesOrders			INT				NOT NULL IDENTITY PRIMARY KEY,
		idSalesOrders				INT				NOT NULL,
		idProduct					INT				NOT NULL,
		vlItemSalesOrders			NUMERIC(10,2)	NOT NULL							
	);
	ALTER TABLE dbo.tbItemSalesOrders ADD CONSTRAINT fk_tbItemSalesStore_tbSalesOrders FOREIGN KEY (idSalesOrders) REFERENCES tbSalesOrders(idSalesOrders);
	ALTER TABLE dbo.tbItemSalesOrders ADD CONSTRAINT fk_tbItemSalesStore_tbProduct FOREIGN KEY (idProduct) REFERENCES tbProduct(idProduct);
END
GO

--############################################################################################################################

-- PARTE 02 > DML

-- Carga tbProduct
INSERT tbProduct (NmProduct, vlProduct)
VALUES	('Gol', 35000), ('Saveiro', 50000), ('Cruze', 80000), ('S10', 110000), ('Fusca', 22000), 
		('Opala', 70000), ('Onix', 35000), ('Uno', 22000), ('Palio', 56000), ('Fiesta', 85000), ('Ferrari', 1500000), 	
		('Strata', 72000), ('Toro', 80000), ('Corolla', 110000), ('HB20', 110000), ('Sandero', 22000), 
		('Voyage', 73000), ('Polo', 59000), ('Hilux', 22000), ('Logan', 51000), ('Ranger', 85000), ('Fox', 42000),
		('Versa', 78000), ('Civic', 99000), ('Virtus', 67000), ('Duster', 92000), ('T-Cross', 77000)
GO

-- Carga tbStore
INSERT tbStore (NmStore, idCep)
VALUES
	('DeCar', CAST(RAND() * 10634 AS INTEGER)),
	('AutoCar', CAST(RAND() * 10634 AS INTEGER)),
	('BomCar', CAST(RAND() * 10634 AS INTEGER)),
	('Senhor dos Carros', CAST(RAND() * 10634 AS INTEGER)),
	('Imp�rio dos Carros', CAST(RAND() * 10634 AS INTEGER)),
	('Mercad�o dos Carros', CAST(RAND() * 10634 AS INTEGER)),
	('Carro de Primeira', CAST(RAND() * 10634 AS INTEGER)),
	('MotoCar', CAST(RAND() * 10634 AS INTEGER)),
	('Feira dos Carros', CAST(RAND() * 10634 AS INTEGER)),
	('Shopping dos Carros', CAST(RAND() * 10634 AS INTEGER)),
	('Aqui tem Carro', CAST(RAND() * 10634 AS INTEGER)),
	('Belo Carro', CAST(RAND() * 10634 AS INTEGER)),
	('FastCar', CAST(RAND() * 10634 AS INTEGER)),
	('Top Ve�culos', CAST(RAND() * 10634 AS INTEGER)),
	('Imp�rio dos Ve�culos', CAST(RAND() * 10634 AS INTEGER)),
	('Primeira Linha Ve�culos', CAST(RAND() * 10634 AS INTEGER)),
	('Bom Ve�culos', CAST(RAND() * 10634 AS INTEGER)),
	('Carros do Z�', CAST(RAND() * 10634 AS INTEGER)),
	('Boa Estrada Ve�culos', CAST(RAND() * 10634 AS INTEGER)),
	('Feir�o Ve�culos', CAST(RAND() * 10634 AS INTEGER)),
	('S� Ve�culos', CAST(RAND() * 10634 AS INTEGER)),
	('Inter Car', CAST(RAND() * 10634 AS INTEGER)),
	('Bom Pre�o Ve�culos', CAST(RAND() * 10634 AS INTEGER)),
	('De A a Z Ve�culos', CAST(RAND() * 10634 AS INTEGER)),
	('Bel Ve�culos', CAST(RAND() * 10634 AS INTEGER)),
	('Import Ve�culos', CAST(RAND() * 10634 AS INTEGER)),
	('Import Car', CAST(RAND() * 10634 AS INTEGER)),
	('MultiCar', CAST(RAND() * 10634 AS INTEGER)),
	('Bom Neg�cio Ve�culos', CAST(RAND() * 10634 AS INTEGER)),
	('Start Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Aldos Car Multimarcas', CAST(RAND() * 10634 AS INTEGER)),
	('Amoreira Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Apolo Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Arco-�ris Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('BlackDog Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Buzina Autom�veis', CAST(RAND() * 10634 AS INTEGER)),
	('Caiaque Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Capacidade Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Caridoso Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Carros & Carros Multimarcas', CAST(RAND() * 10634 AS INTEGER)),
	('Catapulta Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Cativo Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Cipauto Chevrolet', CAST(RAND() * 10634 AS INTEGER)),
	('Colossal Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Confi�vel Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Destak Ve�culos', CAST(RAND() * 10634 AS INTEGER)),
	('Dire��o Autom�veis', CAST(RAND() * 10634 AS INTEGER)),
	('EagleEye Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Elite Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Equipe Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Espa�o Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Flourish Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Fun��o Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Grande Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Greater Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Humano Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('IronHorse Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Jovel Autom�veis', CAST(RAND() * 10634 AS INTEGER)),
	('Kudos Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Louco por Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Luciano Ve�culos', CAST(RAND() * 10634 AS INTEGER)),
	('Marcha R� Autom�veis', CAST(RAND() * 10634 AS INTEGER)),
	('Melhores do Asfalto Ve�culos', CAST(RAND() * 10634 AS INTEGER)),
	('Meta Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Meu Carro Seminovos e Novos', CAST(RAND() * 10634 AS INTEGER)),
	('Motor Potente Multimarcas', CAST(RAND() * 10634 AS INTEGER)),
	('Motorizado Multimarcas', CAST(RAND() * 10634 AS INTEGER)),
	('No Volante Multimarcas', CAST(RAND() * 10634 AS INTEGER)),
	('O Carro Multimarcas', CAST(RAND() * 10634 AS INTEGER)),
	('O Motorista Ve�culos', CAST(RAND() * 10634 AS INTEGER)),
	('OneSource Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('OnTime Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Patriota Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Perfect Car Ve�culos', CAST(RAND() * 10634 AS INTEGER)),
	('Pilot Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Pontes Ve�culos', CAST(RAND() * 10634 AS INTEGER)),
	('Promo Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Provis�o Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('P�tio Ve�culos', CAST(RAND() * 10634 AS INTEGER)),
	('Resoluto Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Roda Quente Ve�culos', CAST(RAND() * 10634 AS INTEGER)),
	('Rodado Autom�veis', CAST(RAND() * 10634 AS INTEGER)),
	('SemiNovos Unidas', CAST(RAND() * 10634 AS INTEGER)),
	('Sinergia Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Sistem�tico Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('S� Carr�o', CAST(RAND() * 10634 AS INTEGER)),
	('S� Ve�culos', CAST(RAND() * 10634 AS INTEGER)),
	('Tango Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Top Ve�culos', CAST(RAND() * 10634 AS INTEGER)),
	('Tri Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Triunfo Multimarcas', CAST(RAND() * 10634 AS INTEGER)),
	('Tudo de Carro Multimarcas', CAST(RAND() * 10634 AS INTEGER)),
	('Un Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Underground Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Volante Quente Autom�veis', CAST(RAND() * 10634 AS INTEGER)),
	('Web Motors', CAST(RAND() * 10634 AS INTEGER)),
	('Wiki Veiculos', CAST(RAND() * 10634 AS INTEGER)),
	('Z� dos Ve�culos', CAST(RAND() * 10634 AS INTEGER))
GO

-- Carga tbCustomer
BEGIN
	-- Script gera CPF e CNPJ
	-- Script e logica retirados do post do Dirceu Resende (https://www.dirceuresende.com/blog/gerador-de-cpf-e-cnpj-valido-para-testes-de-ambiente-no-sql-server/)
	DECLARE @Quantidade INT
	DECLARE @Fl_Tipo BIT

	SET @Quantidade = 1
	SET @Fl_Tipo = 0 -- Gera CPF
--	SET @Fl_Tipo = 1 -- Gera CNPJ

	IF (OBJECT_ID('tempdb..#Tabela_Final') IS NOT NULL) DROP TABLE #Tabela_Final
	CREATE TABLE #Tabela_Final (Nr_Documento VARCHAR(18))

	DECLARE @n INT
	DECLARE @n1 INT
	DECLARE @n2 INT
	DECLARE @n3 INT
	DECLARE @n4 INT
	DECLARE @n5 INT
	DECLARE @n6 INT
	DECLARE @n7 INT
	DECLARE @n8 INT
	DECLARE @n9 INT
	DECLARE @n10 INT
	DECLARE @n11 INT
	DECLARE @n12 INT
	DECLARE @d1 INT
	DECLARE @d2 INT
    
	IF (@Fl_Tipo = 0)
	BEGIN
		WHILE (@Quantidade > 0)
		BEGIN        
			SET @n = 9
			SET @n1 = CAST(( @n + 1 ) * RAND(CAST(NEWID() AS VARBINARY )) AS INT)
			SET @n2 = CAST(( @n + 1 ) * RAND(CAST(NEWID() AS VARBINARY )) AS INT)
			SET @n3 = CAST(( @n + 1 ) * RAND(CAST(NEWID() AS VARBINARY )) AS INT)
			SET @n4 = CAST(( @n + 1 ) * RAND(CAST(NEWID() AS VARBINARY )) AS INT)
			SET @n5 = CAST(( @n + 1 ) * RAND(CAST(NEWID() AS VARBINARY )) AS INT)
			SET @n6 = CAST(( @n + 1 ) * RAND(CAST(NEWID() AS VARBINARY )) AS INT)
			SET @n7 = CAST(( @n + 1 ) * RAND(CAST(NEWID() AS VARBINARY )) AS INT)
			SET @n8 = CAST(( @n + 1 ) * RAND(CAST(NEWID() AS VARBINARY )) AS INT)
			SET @n9 = CAST(( @n + 1 ) * RAND(CAST(NEWID() AS VARBINARY )) AS INT)
			SET @d1 = @n9 * 2 + @n8 * 3 + @n7 * 4 + @n6 * 5 + @n5 * 6 + @n4 * 7 + @n3 * 8 + @n2 * 9 + @n1 * 10
			SET @d1 = 11 - ( @d1 % 11 )

			IF ( @d1 >= 10 )
				SET @d1 = 0

			SET @d2 = @d1 * 2 + @n9 * 3 + @n8 * 4 + @n7 * 5 + @n6 * 6 + @n5 * 7 + @n4 * 8 + @n3 * 9 + @n2 * 10 + @n1 * 11
			SET @d2 = 11 - ( @d2 % 11 )

			IF ( @d2 >= 10 )
				SET @d2 = 0

			INSERT INTO #Tabela_Final
			SELECT CAST(@n1 AS VARCHAR) + CAST(@n2 AS VARCHAR) + CAST(@n3 AS VARCHAR) + '.' + CAST(@n4 AS VARCHAR) + CAST(@n5 AS VARCHAR) + CAST(@n6 AS VARCHAR) + '.' + CAST(@n7 AS VARCHAR) + CAST(@n8 AS VARCHAR) + CAST(@n9 AS VARCHAR) + '-' + CAST(@d1 AS VARCHAR) + CAST(@d2 AS VARCHAR)

			SET @Quantidade = @Quantidade - 1 
		END
	END

	-- Gera 1000000 clientes (Com nomes aleatorios)
	DECLARE @x INT
	SET		@x = 1

	WHILE (@x < 1000000)
	BEGIN
		INSERT dbo.tbCustomer (idCep, NmCustomer, dsCPF, dsSexo, dtLastUpdate)
		SELECT TOP 1
			CAST(RAND() * 10634 AS INTEGER),
			[Column 1],
			1,
			'M',
			GETDATE()
		FROM dbo.nomes -- tabela com nomes italianos (retirados da internet)
		ORDER BY newid()
	
		SET @x = (@x + 1)
	 END
END
GO