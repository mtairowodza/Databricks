# Databricks notebook source
# MAGIC %md
# MAGIC ###workflow for nunning ochestration of workloads
# MAGIC ####supports BI and SQL workloads on Delta lake
# MAGIC ####intergration with Bi tools
# MAGIC ####direct querying of data
# MAGIC ####natural language to write AI queries
# MAGIC ####DI engine support natural language to write sql queries
# MAGIC ####AI driven serverless computing - cost serving and peak perfoming speeds
# MAGIC ####AI supporting debugging
# MAGIC ####unity catalogue- unified governance and AI
# MAGIC ####unity catalog supoort data leneage, access controls and governance
# MAGIC ####delta lake - optimised storage layer  provides foundation for storing data and tables in the databricks data intelligence platform
# MAGIC ####extends parquet datafiles with transaction log for ACID transactions and scalable metadata handling

# COMMAND ----------

# MAGIC %md
# MAGIC repos allow integration with gi leads to maintanace of immutable recod of changes/ history -repos provide external intergration points to suppot a CI/CD pipeline -Dbricks suports Azure, Devops, Bitbucket, Github and Gitlab -bo support of privates gits
# MAGIC
# MAGIC clone repository: create a local working copy of a remote repository -Pull: synhcronise upstream changes with local copy -create: create a new item to your local copy -move- move or rename existing items of your local copy -commit and push to synhcronise local changes with your local copyu -create branch- create code base without impact other branches in the repository -avoid interfering with the main.

# COMMAND ----------

# MAGIC %md
# MAGIC #basic compte structures
# MAGIC
# MAGIC ##Clusters
# MAGIC -made up of one or more VM instances
# MAGIC - distributes workloads accross workers
# MAGIC
# MAGIC - Driver coordinates activites of executors
# MAGIC - Workers run tasts composing a spark job
# MAGIC
# MAGIC #Three types of compute
# MAGIC - All purpose clusters:  clusters for interactive development
# MAGIC - job clusters: automating workloads
# MAGIC -Sql Warehouses: compute to run DBSQL queries and dashboards
# MAGIC
# MAGIC #houw to create all purpose clusters
# MAGIC -through interactive notebooks
# MAGIC -through workspace
# MAGIC -programmatical using the commmand prompt line interface or rest API
# MAGIC Job cluster
# MAGIC
# MAGIC #job clusters
# MAGIC -running automated jobs
# MAGIC -job scheduler creates new job cluster and terminates when job is complete
# MAGIC -cannot restart a job cluster
# MAGIC
# MAGIC
# MAGIC #SQL warehouses
# MAGIC
# MAGIC -DBSQL queries and updating dashboards
# MAGIC -single nodes - low cost single instance for single node machine learning workloads and lightweight exploratory analysis
# MAGIC Mlti-node-default for workloads developed in any supported language -  VM instance for the driver and at least one additional instance for workers
# MAGIC
# MAGIC -must use appropreate runtime for each one
# MAGIC DB runtime has essential software components eg apache spark , machine learning workloads, 
# MAGIC
# MAGIC Photon:  The next generation engine on Databricks DI platform 
# MAGIC    - cheaper and faster
# MAGIC    - buildt for all use cases
# MAGIC    - No code changes- can be used seemlessly with db runtime without any code changes
# MAGIC    - data ingestion, ETL and streaming, data science and interactive queries directly on your datalake
# MAGIC
# MAGIC
# MAGIC #daabricks sql warehouses 
# MAGIC   - to execute queries and business applications at scale
# MAGIC   
# MAGIC   -Classic SQL warehouses:
# MAGIC      - compute resources run in your cloud account
# MAGIC      -takes several minutes to start-up
# MAGIC      -classic compute plane hasnatural isolation
# MAGIC      -natural isolation
# MAGIC      -exploratory SQL workloads
# MAGIC      -Mandates that uoui contrl your own resources
# MAGIC      -manage own resources including launching and cleaning clusters
# MAGIC      -more control but more effort 
# MAGIC
# MAGIC   - Severless SQL Warehouse
# MAGIC      - Compute resources run in a compute later within your databricks account
# MAGIC      - rapid startup time 2-6secs
# MAGIC      runs withing the network boundary in the workspace
# MAGIC      - Best for high-concurrenty BI SQL, and DSML using SQL , data science and machine learning
# MAGIC      -FUlly managed by Databricks
# MAGIC      -privides instant, elastic and zero management compute
# MAGIC      -high performance high concurrency workloads
# MAGIC      - auro stop feature setting
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Databricks Notebooks
# MAGIC
# MAGIC  - Multi-language (python, sql, scal and R)
# MAGIC  - COllaborative
# MAGIC  - Ideal for exploration - explore visualise and summarise data
# MAGIC  - Adaptable - intallation of local libraries and loca modules
# MAGIC  - Reproducible - auto tracking of version history and git contol version
# MAGIC  - Faster production time schedule notebooks as jobs or create dashboards
# MAGIC  - Enterprise ready access controls, ID management, auditability 

# COMMAND ----------

##Running notebooks

print('.ptint hallo')

# COMMAND ----------

# MAGIC %md
# MAGIC # Partner Learning
# MAGIC This is some text in **bold** and _italic_. and *here*
# MAGIC - Here's a bullet point

# COMMAND ----------

# MAGIC %md
# MAGIC ## DB workspace and services
# MAGIC - Implemantationn of lakehouse technology
# MAGIC - data storage layer on top of data lake, flexible, cost efficient, storage
# MAGIC - best of both worlds
# MAGIC - unified and scalable
# MAGIC
# MAGIC ### Data plane
# MAGIC - is where data is processed
# MAGIC - hosts compute resources - clusters
# MAGIC - connection to external data sources
# MAGIC
# MAGIC
# MAGIC ### Control Plane
# MAGIC - back end services that data bricks manages
# MAGIC - metastore, access control, data lineage / explorer
# MAGIC - launch clusters, run jobs etc
# MAGIC - workflow managers
# MAGIC - cluster managers for compute
# MAGIC - unity catalogue
# MAGIC
# MAGIC ### Web app
# MAGIC - three services, data science, data bricks machine learning, and sql learning persona
# MAGIC - using reps
# MAGIC - Ochestration has 2 components. 1. Delta live tables and 2. Jobs
# MAGIC
# MAGIC ## Clusters
# MAGIC - Compute resources and configurations - can run as commands in notebooks or as a job
# MAGIC - live in dataplace in org's account
# MAGIC - a cluster has a Driver node and worker node, each being a VM instance
# MAGIC - 2 main types
# MAGIC -  All-purpose for interactive development using notebooks. Created from wordspace or API, multiple sers can share 
# MAGIC -  and job clusters for automating workloads, created when running job. cant be restarted. Configuration infor retained for the last 30 instances
# MAGIC - Workloads includes notebook, job, pipeline
# MAGIC
# MAGIC ### cluster mode
# MAGIC - standard (multi-node)  - default mode for workloads supports any language
# MAGIC - single node -low cost, single node machine, lightweight analysis and machine learning
# MAGIC
# MAGIC ## Runtime versions
# MAGIC
# MAGIC - standard  - Apache spark and other components
# MAGIC - machne learning - has popular machine learning libraries
# MAGIC - Optional add on to optimize performance and cost of SQL workloads
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # DATA Bricks notebooks
# MAGIC
# MAGIC - support multiple languages - python, SQL, Scala, R
# MAGIC - collaborative - can be used by multiple users
# MAGIC - deal for exploration
# MAGIC - adaptable- install libraries
# MAGIC - reproducable
# MAGIC - faster to production - jobs ad visualisation 
# MAGIC - enterprise ready
# MAGIC
# MAGIC - magic commands for switchinig languages, shell code, md or db utils
# MAGIC - execute one notebook from another using %run
# MAGIC - 

# COMMAND ----------

# MAGIC %md
# MAGIC # GIT versioning with Databricks Repos
# MAGIC - native intergration with Github, Azure devops, gitlab and Gitbucket
# MAGIC - CI/CD API surface to intergrate with automation for deve/staging/prod multi workspace story
# MAGIC - Enterprise read - Allow lists to avoid exfiltration  and secret detection to avoid leaking keys
# MAGIC - cloning repository creates a local working copy of the remote git repository
# MAGIC - Synhcronising upstream
# MAGIC - creating branches
# MAGIC - Requires username and personal access token
# MAGIC - 

# COMMAND ----------


