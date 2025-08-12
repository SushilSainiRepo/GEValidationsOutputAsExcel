Pre-Requisites
- Install Docker on WSL
- Ensure Docker Daemon `dockerd` is running; validate by running `docker ps`

Install recommended extentions

Reopen folder in WSL

run docker 
sudo dockerd

Build image by running the following command

`sudo docker build -t ge-validation .`

in case you already have above and want to rebuild a latest image from project 
remove image 
sudo docker  rmi ge-validation

Execute validations by running the following command. (NOTE: replace `/mnt/c/validation/files/` with your own location for bot volumes)


`sudo docker  run --name gevalidation --rm -v  /mnt/c/validation/files/data-docs:/app/great_expectations/uncommitted/data_docs/local_site/ -v  /mnt/c/validation/files/newinputfiles:/app/DynamicValidation/files -v  /mnt/c/validation/files/metadata:/app/DynamicValidation/metadata  ge-profitability-validation`



Following command keep the container running and then
RUN above command without --rm in it so that container is not removed , then use following comands to start and execute to reproduce expectations and run the validations manually 
`sudo docker  run --name filevalidation  -v  /mnt/c/validation/files/data-docs:/app/great_expectations/uncommitted/data_docs/local_site/ -v  /mnt/c/validation/files/newinputfiles:/app/DynamicValidation/files -v  /mnt/c/validation/files/metadata:/app/DynamicValidation/metadata  ge-validation tail -f /dev/null`


If contaner is not already started, then run following to generate expecations 

`sudo docker start 'filevalidation'`
`sudo docker exec -it 'filevalidation'  bash`
`python3 ./DynamicValidation/genExpectations.py`
`sudo docker stop filevalidation`

if you wan to run validations 

`sudo docker start 'filevalidation'`
`sudo docker exec -it 'filevalidation'  bash`
`python3 ./DynamicValidation/invoke_save_validations.py`


or run to create expecations and validate in one go 
`sudo docker start 'profitfilevalidation'`
`sudo docker exec -it 'profitfilevalidation'  bash -c "python3 ./DynamicValidation/genExpectations.py;python3 ./DynamicValidation/invoke_save_validations.py`

to stop container
`sudo docker stop profitfilevalidation`


to remove container
`sudo docker rm profitfilevalidation`

to cleanup the files(cli command)
`great_expectations docs clean -s local_site`
