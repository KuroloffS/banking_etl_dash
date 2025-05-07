USE master;
IF DB_ID(N'BankingAnalytics') IS NOT NULL BEGIN
    ALTER DATABASE BankingAnalytics SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE BankingAnalytics;
END;
CREATE DATABASE BankingAnalytics;
GO
USE BankingAnalytics;
GO
CREATE TABLE dbo.[cards] (
    [id] NVARCHAR(4000),
    [user_id] NVARCHAR(4000),
    [card_number] NVARCHAR(4000),
    [balance] BIGINT,
    [created_at] NVARCHAR(4000),
    [card_type] NVARCHAR(4000),
    [limit_amount] BIGINT,
    [source_file] NVARCHAR(4000),
    [is_blocked] BIT,
    PRIMARY KEY ([id])
);
GO
CREATE TABLE dbo.[logs] (
    [id] NVARCHAR(4000),
    [transaction_id] NVARCHAR(4000),
    [message] NVARCHAR(4000),
    [created_at] NVARCHAR(4000),
    [source_file] NVARCHAR(4000),
    PRIMARY KEY ([id])
);
GO
CREATE TABLE dbo.[reports] (
    [id] NVARCHAR(4000),
    [report_type] NVARCHAR(4000),
    [created_at] NVARCHAR(4000),
    [total_transactions] NVARCHAR(4000),
    [flagged_transactions] NVARCHAR(4000),
    [total_amount] NVARCHAR(4000),
    [source_file] NVARCHAR(4000),
    PRIMARY KEY ([id])
);
GO
CREATE TABLE dbo.[scheduled_payments] (
    [id] NVARCHAR(4000),
    [user_id] NVARCHAR(4000),
    [card_id] NVARCHAR(4000),
    [amount] NVARCHAR(4000),
    [payment_date] NVARCHAR(4000),
    [status] NVARCHAR(4000),
    [created_at] NVARCHAR(4000),
    [source_file] NVARCHAR(4000),
    PRIMARY KEY ([id])
);
GO
CREATE TABLE dbo.[transactions] (
    [id] NVARCHAR(4000),
    [from_card_id] NVARCHAR(4000),
    [to_card_id] NVARCHAR(4000),
    [amount] BIGINT,
    [status] NVARCHAR(4000),
    [created_at] DATETIME2,
    [transaction_type] NVARCHAR(4000),
    [source_file] NVARCHAR(4000),
    [is_flagged] BIT,
    PRIMARY KEY ([id])
);
GO
CREATE TABLE dbo.[users] (
    [id] NVARCHAR(4000),
    [name] NVARCHAR(4000),
    [phone_number] NVARCHAR(4000),
    [email] NVARCHAR(4000),
    [created_at] DATETIME2,
    [last_active_at] NVARCHAR(4000),
    [is_vip] BIT,
    [total_balance] BIGINT,
    [source_file] NVARCHAR(4000),
    [phone_number_issue] NVARCHAR(4000),
    [email_issue] NVARCHAR(4000),
    PRIMARY KEY ([id])
);
GO
CREATE TABLE dbo.retrieveinfo (
    retrieve_id  INT IDENTITY PRIMARY KEY,
    source_file  NVARCHAR(64),
    retrieved_at DATETIME2  DEFAULT SYSDATETIME(),
    total_rows   INT,
    processed_rows INT,
    errors       INT,
    notes        NVARCHAR(MAX)
);
GO
SELECT * FROM BankingAnalytics.dbo.retrieveinfo ORDER BY retrieve_id DESC;