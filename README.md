# luigi_gdb_pipeline_demo
An example to illustrate using Luigi to manage a data science workflow in Greenplum Database


Steps to running: 

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
