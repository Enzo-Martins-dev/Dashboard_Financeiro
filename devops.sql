CREATE TABLE users(
	user_id INTEGER PRIMARY KEY,
	current_age SMALLINT NOT NULL,
	retirement_age SMALLINT NOT NULL,
	birth_year SMALLINT NOT NULL,
	birth_month SMALLINT NOT NULL,
	gender VARCHAR(10)
);

CREATE TABLE finances(
	user_id_fk INTEGER REFERENCES users(user_id) UNIQUE NOT NULL,
	per_capita_income VARCHAR(15) NOT NULL,
	yearly_income VARCHAR(15) NOT NULL,
	total_debt VARCHAR(15) NOT NULL,
	credit_score VARCHAR(15) NOT NULL,
	num_credit_cards SMALLINT NOT NULL
);

CREATE TABLE adresses(
	adress_id SERIAL PRIMARY KEY,
	num INTEGER NOT NULL,
	street VARCHAR(50) NOT NULL,
	lat INTEGER NOT NULL,
	long INTEGER NOT NULL,
	user_id_fk INTEGER REFERENCES users(user_id) NOT NULL
);

CREATE TABLE users_adresses(
	user_id_fk INTEGER REFERENCES users(user_id) NOT NULL,
	adress_id_fk INTEGER REFERENCES adresses(adress_id) NOT NULL
);

CREATE TABLE merchants(
	merchant_id INTEGER PRIMARY KEY,
	merchant_city VARCHAR(20) NOT NULL,
	merchant_state VARCHAR(20) NOT NULL,
	zip INTEGER
);

CREATE TABLE cards(
	card_id INTEGER PRIMARY KEY,
	client_id INTEGER REFERENCES users(user_id) NOT NULL, 
	card_brand VARCHAR(20) NOT NULL,
	card_type VARCHAR(20) NOT NULL,
	card_number BIGINT NOT NULL,
	expires DATE NOT NULL,
	CVV SMALLINT NOT NULL,
	has_chip BOOLEAN NOT NULL,
	num_cards_issued SMALLINT NOT NULL,
	credit_limit VARCHAR(15) NOT NULL, --Pelo que vi até os cartões de débito tem limite de crédito
	acct_open_date DATE NOT NULL,
	year_pin_last_changed SMALLINT, --Senha pode nunca ter sido trocada
	card_on_dark_web BOOLEAN NOT NULL
);

CREATE TABLE transactions2(
	id INTEGER PRIMARY KEY,
	date DATE,
	client_id INTEGER REFERENCES users(id) NOT NULL, 
	card_id INTEGER REFERENCES cards(id) NOT NULL,
	amount VARCHAR(10) NOT NULL,
	use_chip VARCHAR(20) NOT NULL,
	merchant_id INTEGER REFERENCES merchants(merchant_id) NOT NULL,
	mcc NUMERIC(8, 2),
	errors VARCHAR(100)
);
SELECT * FROM df_merchants
WHERE merchant_id = 26462

SELECT * FROM merchants
WHERE merchant_id = 26462

SELECT * FROM transactions2
LIMIT 100

SELECT * FROM transactions
WHERE merchant_id = 26462
LIMIT 100


CREATE TABLE transaction_errors(
	transaction_id INTEGER REFERENCES transactions(transaction_id) NOT NULL,
	error TEXT
);


CREATE TABLE log_transacoes(
	id INTEGER PRIMARY KEY,
	date DATE,
	client_id INTEGER REFERENCES users(id) NOT NULL, 
	card_id INTEGER REFERENCES cards(id) NOT NULL,
	amount VARCHAR(10) NOT NULL,
	use_chip VARCHAR(20) NOT NULL,
	merchant_id INTEGER REFERENCES merchants(merchant_id) NOT NULL,
	mcc NUMERIC(8, 2),
	errors VARCHAR(100)
)
SELECT * FROM cards

SELECT * FROM tran

SELECT COUNT(DISTINCT id) FROM adresses
SELECT COUNT(DISTINCT id) FROM cards 

SELECT COUNT(DISTINCT id) FROM finances


WHERE id = 4524

CREATE TABLE log_erros(
	transaction_id INTEGER REFERENCES transactions(id) NOT NULL,
	error TEXT,
	error_date TIMESTAMP NOT NULL
)

CREATE TABLE log_users(
	user_id INTEGER PRIMARY KEY,
	current_age SMALLINT NOT NULL,
	retirement_age SMALLINT NOT NULL,
	birth_year SMALLINT NOT NULL,
	birth_month SMALLINT NOT NULL,
	gender VARCHAR(10),
	last_modified TIMESTAMP NOT NULL
)




CREATE OR REPLACE FUNCTION log_transacoes() 
RETURNS TRIGGER AS $$
BEGIN 
	INSERT INTO log_transacoes(id, date, client_id, card_id, amount, use_chip, merchant_id, mcc, errors)
	VALUES (NEW.id, NOW(), NEW.client_id, NEW.card_id, NEW.amount, NEW.use_chip, NEW.merchant_id, NEW.mcc, NEW.errors);

	RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER novas_transacoes
AFTER INSERT ON transactions
FOR EACH ROW
EXECUTE FUNCTION log_transacoes();

SELECT * FROM user_adress

SELECT * FROM users

SELECT * FROM adresses

ALTER TABLE users
ADD CONSTRAINT transactions_pkey PRIMARY KEY (id);
DELETE FROM users
WHERE id IN (
    SELECT id
    FROM (
        SELECT id, ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) AS row_num
        FROM users
    ) t
    WHERE t.row_num > 1
);

DELETE FROM users WHERE id IS NULL;

SELECT * FROM log_transacoes

INSERT INTO transactions(id, date, client_id, card_id, amount, use_chip, merchant_id, mcc, errors)
	VALUES (74753231,NOW(),1556,2972,'$-77.00','Swipe Transaction',59935,5499, 'AAAAAAAAAAAA')

SELECT * FROM transactions
LIMIT 100

ALTER TABLE transactions 
DROP COLUMN merchant_city,
DROP COLUMN merchant_state,
DROP COLUMN zip,
DROP COLUMN mcc;


SELECT * FROM transactions
WHERE errors IS NOT NULL
ORDER BY errors ASC
LIMIT 100

SELECT * FROM merchants

SELECT * FROM transactions_errors



CREATE OR REPLACE FUNCTION log_transacoes() 
RETURNS TRIGGER AS $$
BEGIN 
	INSERT INTO log_transacoes(transaction_id, client_id,transaction_date_time, card_id, amount, use_chip, mcc, merchant_id)
	VALUES (NEW.transaction_id, NEW.client_id,
	NOW(), NEW.card_id, NEW.amount, NEW.use_chip, 
	NEW.mcc, 
	NEW.merchant_id);

	RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER novas_transacoes
AFTER INSERT ON transactions
FOR EACH ROW
EXECUTE FUNCTION log_transacoes();


transaction_id INTEGER REFERENCES transactions(id) NOT NULL,
	error TEXT,
	error_date TIMESTAMP NOT NULL
SELECT * FROM log_erros


CREATE TABLE log_erros(
    transaction_id INTEGER REFERENCES transactions(id) NOT NULL,
    error TEXT,
    error_date TIMESTAMP NOT NULL
);


CREATE OR REPLACE FUNCTION log_erros() 
RETURNS TRIGGER AS $$
BEGIN 
    INSERT INTO log_erros(transaction_id, error, error_date)
    VALUES (NEW.id, NEW.errors, NOW());

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE TRIGGER auditoria_erros
AFTER INSERT ON transactions_errors
FOR EACH ROW
EXECUTE FUNCTION log_erros();

transaction_id INTEGER REFERENCES transactions(id) NOT NULL,
	error TEXT

SELECT * FROM transactions_errors
ORDER BY errors DESC

INSERT INTO transactions_errors(id, errors)
VALUES (74753231, 'AAAAAAAAAAAA');

SELECT * FROM log_erros


CREATE OR REPLACE FUNCTION log_users() 
RETURNS TRIGGER AS $$
BEGIN 
    INSERT INTO log_users(user_id, current_age, retirement_age,birth_year, birth_month, gender, last_modified)
    VALUES (NEW.id, NEW.current_age, NEW.retirement_age,NEW.birth_year, NEW.birth_month, NEW.gender, NOW());

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM log_users

-- user_id INTEGER PRIMARY KEY,
-- 	current_age SMALLINT NOT NULL,
-- 	retirement_age SMALLINT NOT NULL,
-- 	birth_year SMALLINT NOT NULL,
-- 	birth_month SMALLINT NOT NULL,
-- 	gender VARCHAR(10),
-- 	last_modified TIMESTAMP NOT NULL

CREATE OR REPLACE TRIGGER auditoria_users
AFTER INSERT ON users
FOR EACH ROW
EXECUTE FUNCTION log_users();

SELECT * FROM log_users

INSERT INTO users (id,current_age, retirement_age, birth_year, birth_month, gender)
VALUES (90000,30, 65, 1993, 5, 'Male');

SELECT * FROM transacoes_por_usuario

CREATE VIEW transacoes_por_usuario AS
SELECT 
    u.id AS user_id,
    u.current_age,
    u.retirement_age,
    u.gender,
    t.id AS transaction_id,
    t.date AS transaction_date,
    t.amount AS transaction_amount,
    t.use_chip,
    c.card_brand,
    m.merchant_city,
    m.merchant_state
FROM public.transactions t
JOIN public.users u ON u.id = t.client_id
JOIN public.cards c ON c.id = t.card_id
JOIN public.merchants m ON m.merchant_id = t.merchant_id;


CREATE VIEW transacoes_com_erros AS
SELECT 
    t.id AS transaction_id,
    t.date AS transaction_date,
    t.amount AS transaction_amount,
    t.errors,
    l.error_date
FROM public.transactions_errors te
JOIN public.transactions t ON t.id = te.id
JOIN public.log_erros l ON l.transaction_id = t.id
WHERE t.errors IS NOT NULL;


CREATE VIEW transacoes_por_mes AS
SELECT 
    EXTRACT(YEAR FROM t.date) AS year,
    EXTRACT(MONTH FROM t.date) AS month,
    COUNT(t.id) AS total_transactions,
    SUM(t.amount::numeric) AS total_amount
FROM public.transactions t
GROUP BY year, month
ORDER BY year, month;

CREATE VIEW usuarios_com_multipos_cartoes AS
SELECT 
    c.client_id AS user_id,
    COUNT(c.id) AS num_cards
FROM public.cards c
GROUP BY c.client_id
HAVING COUNT(c.id) > 1;
