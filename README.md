# luigi_gdb_pipeline_demo
An example to illustrate using Luigi to manage a data science workflow in Greenplum Database

## Use Case Description

The use case for this demo is a simple unsupervised approach for identifying anomalous or highly variant user network behavior based on activity patterns.  The main idea is to create 24 matrices, one for each hour of the day.  The rows of the index are indexed by network users and the columns by days of the week.  Row (i,j) for the matrix corresponding to hour k counts the number of events from user i in hour k of day j.  The reason for creating a matrix for each hour is to accounts for differing volumes of traffic throughout the day.  We then run principal component analysis (PCA) on each of these matrices and examine the large entries of the principal component directions which will correspond to ‘erratic’ users who drive most of the variance in the data.  In later iterations we can extend the pipeline to use PCA for dimension reduction, clustering and other more sophisticated methods.

We break up our workflow into 5 types of tasks Luigi tasks as follows:

1. Initializing all the necessary schema and user defined functions in our pipeline
2. Creating a table counting the number of flows for each user during each hour of the data set
3. For each of the 24 hours we  use the output of step 2.)  to create a table that is a sparse matrix representation of the hourly flow count matrix.
4.  Run the MADlib PCA function to compute the principal direction for each of the 24 flow count matrices.  The MADlib PCA algorithm is computed in parallel across GPDB nodes.
5. For each of 24 principal directions, Identify users with large entries and write a list of these users with outlier score (magnitude of the entry) to a file to investigate.   

##Steps to running: 

1. Install Pivotal Greenplum Sandbox Virtual Machine.  Available for free download on the [Pivotal Network]
(https://network.pivotal.io/products/pivotal-gpdb#/releases/567/file_groups/337).
 Once we start the Sandbox with VirtualBox or VMware Fusion, it is a two step process to start the GPDB: 
  1. Login as gpadmin with the password provided at startup.  
  2. Type ./start_all.sh to start the GPDB instance.
2.  Create an ssh tunnel to make the GPDB instance available to a psycopg2 connection on localhost at port 9912
  * $ssh gpadmin@\<VM IP address\> -L 9912:127.0.0.1:5432
3.  Set environment variables for scripts to point to GPDB
  * export GPDB_DATABASE=gpadmin
	* export GPDB_USER=gpadmin
	* export GPDB_PASSWORD=\<vm_password\>
	* export GPDB_HOST=localhost
	* export GPDB_PORT=9912
  
4. Run [write_sample_data_to_db.py](https://github.com/ericwayman/luigi_gdb_pipeline_demo/blob/master/generate_sample_data/write_sample_data_to_db.py)
To populate database with fake data.

5.  Run the Luigi Pipeline.  From inside pca_pipeline/
  1. $luigid —background
  2. $python -m luigi —module pipeline PipelineTask
  
We can view the status of our pipeline and the pipeline dependency graph by opening a browser to http://localhost:8082
After running the pipeline, by default the output files will be available in pca_pipeline/target.
