library(RPostgres)
library(DBI)
library(httr2)
library(tidyverse)

# file with my Postgres credentials
source("credentials.R")
# the files looks like this but with my information in
# cred_psql_docker <- list(dbname = "postgres",
#                          host   = "postgres",
#                          user   = 'my_username',
#                          pass   = 'my_password',
#                          port   = 5432)

# file used to send queries to Postgres
source("psql_queries.R")
# the file contains the following code
# To get data from Postgres to R
# psql_select <- function(cred, query_string){
#   con_postgre <- DBI::dbConnect(RPostgres::Postgres(),
#                                 dbname = cred$dbname,
#                                 host   = cred$host,
#                                 user      = cred$user,
#                                 pass      = cred$pass,
#                                 port = cred$port)
#   
#   query_res <- dbSendQuery(con_postgre, query_string)
#   query_res_out <- dbFetch(query_res, n = -1)
#   dbClearResult(query_res)
#   dbDisconnect(con_postgre)
#   return(query_res_out)
# }
# # To manipulate data in Postgres from R. E.g., create schemas, tables, insert 
# # rows or update values
# psql_manipulate <- function(cred, query_string){
#   con_postgre <- DBI::dbConnect(RPostgres::Postgres(),
#                                 dbname = cred$dbname,
#                                 host   = cred$host,
#                                 user      = cred$user,
#                                 pass      = cred$pass,
#                                 port = cred$port)
#   
#   query_res <- dbSendStatement(con_postgre, query_string)
#   return(paste0("Satement completion: ", dbGetInfo(query_res)$has.completed))
#   dbClearResult(query_res)
#   dbDisconnect(con_postgre)
# }
# # To insert entire dataframes in Postgres tables
# psql_append_df <- function(cred, schema_name, tab_name, df){
#   con_postgre <- DBI::dbConnect(RPostgres::Postgres(),
#                                 dbname = cred$dbname,
#                                 host   = cred$host,
#                                 user      = cred$user,
#                                 pass      = cred$pass,
#                                 port = cred$port)
#   
#   query_res <- dbAppendTable(con = con_postgre, 
#                              name = Id(schema = schema_name, table = tab_name), 
#                              value = df)
#   print(paste0("Number of rows inserted: ",  query_res))
#   dbDisconnect(con_postgre)
# }

################
# QUESTION 1.3 #
################
# creating a schema 
psql_manipulate(cred = cred_psql_docker, 
                query_string = "CREATE SCHEMA exam;")

# creating table in the exam schema with the specifed columns and datatypes
psql_manipulate(cred = cred_psql_docker, 
                query_string = "create table exam.figure1 (
	                              article_id serial primary key,
	                              article_source varchar(200),
	                              article_title varchar(300),
                              	article_imported timestamp(0) without time zone default current_timestamp(0)
                );")

# inserting values into table
psql_manipulate(cred = cred_psql_docker, 
                query_string = "insert into exam.figure1
                                values (default,	'wsj',		'Inflation rises to record high',		default),
			                                 (default,	'ft',		'Oil prices are record high',			default),
			                                 (default,	'nyt',		'High costs of cleaner air',			default)
                ;")

# checking the table
psql_select(cred = cred_psql_docker,
            query_string = "select * from exam.figure1")

# deleting rows where the article_id = 1 or the article_source = ft
psql_manipulate(cred = cred_psql_docker, 
                query_string = "delete from exam.figure1 
                                where 		article_id = 1
                                      or  article_source = 'ft'
                ")

# checking the table
psql_select(cred = cred_psql_docker,
            query_string = "select * from exam.figure1")

# creating an index on the column article_source
psql_manipulate(cred = cred_psql_docker, 
                query_string = "create index idx_article_source
                                on exam.figure1(article_source)
                ;")

# deleting the table
psql_manipulate(cred = cred_psql_docker, 
                query_string = "drop table exam.figure1;
                ")

# checking the table
psql_select(cred = cred_psql_docker,
            query_string = "select * from exam.figure1")
# this gets an error because the table no longer exists