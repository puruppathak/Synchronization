#include <pthread.h>
#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <dirent.h>
#include <string.h>
#include <sys/types.h>


// Name: Puru Pathak
// Multithreading program

//References: https://computing.llnl.gov/tutorials/pthreads/
//          : www.gnu.org
//          : www.stackoverflow.org
//          : www.cprogramming.com




typedef struct Pair_struct{                  //structure used to find key value pair
	char *key;
	char *value;
}Pair_variable;

typedef struct A_Array_struct{                //Associative array struct
	int count;
	Pair_variable *pairs;
}A_Array;

typedef struct Dict_struct{                   //Dictionary struct
    int count;
	A_Array *A_arrays;
}Dict;


int Thread_count;
pthread_mutex_t mutex_lock;
int File_count;
Dict *dict;
char *Dir_name;
int countFiles(DIR *dir);
void printinfo(char *Dir_name);
Dict *createDict(unsigned int capacity);
void *File_parser(void *thread_id);
int insertDict(Dict *dict, const char *key, const char *value);
static unsigned long hash(const char *str);
static Pair_variable * get_pair(A_Array *A_array, const char *key);
int checkDict(const Dict *dict, const char *key);
int ip_count;


int main(int argc, char *argv[]) {
	long i;
	DIR *dir;                                             // Create Directory stream
	FILE *file;                                           // Create File stream
	static const int dict_size = 200;                 
	size_t len = 1000; 
	char *line = NULL;                                    // Initializing line pointer
	size_t read;
	int result;
	char fileName[256];                                     // Initializing File buffer
    Thread_count = atoi(argv[2]);
	
	 
	if(argc > 3) {
		printf("Error: Re-check number of arguments given.\n");             //Checking number of arguments
		return -1;
	}

	if(argc < 3) {
		printf("Error: Re-check number of arguments given.\n");
		return -1;
	}

	Dir_name = argv[1];
	if ((dir = opendir(Dir_name)) == NULL) {
		printf("Error:Re-check directory\n");
		return -1;
	}
		 
	printinfo(Dir_name);
	dict = createDict(dict_size);
	pthread_mutex_init(&mutex_lock, NULL);                 //Initializing Mutex
	File_count = countFiles(dir);
	closedir (dir); 
	pthread_t threads[Thread_count];
	pthread_attr_t attr;
	pthread_attr_init(&attr);                              //Initializing Thread attributes
	
	for (i = 0; i < Thread_count; i++) {                //Task 1 for Assignment: Creating Multiple threads
		printf("Thread created is: %ld\n", i);
		result = pthread_create(&threads[i], &attr, File_parser, (void *)i);
		if (result) {
			printf("Thread creation error\n");
			exit(-1);
		}
	}
	for (i = 0; i < Thread_count; i++) {
		pthread_join(threads[i], NULL);                        //Suspending execution of the caller
	}
	printf("\nTotal unique IPs are: %d\n\n", ip_count);
	pthread_attr_destroy(&attr);
	pthread_mutex_destroy(&mutex_lock);
	pthread_exit(NULL);
	return 0;
}


void printinfo(char *Dir_name)
{

    printf("Program compiled successfully!\n");
	printf("Directory accessed is: %s\n", Dir_name);
}


int checkDict(const Dict *dict, const char *key)
{
	unsigned int index;
	Pair_variable *pair;
	A_Array *A_array;
	if (dict == NULL) {
		return 0;
	}
	if (key == NULL) {
		return 0;
	}
	index = hash(key) % dict->count;
	A_array = &(dict->A_arrays[index]);
	pair = get_pair(A_array, key);
	if (pair == NULL) {
		return 0;
	}
	return 1;
}



void *File_parser(void *thread_id) {
	DIR *dir;                                           //opening a directory stream
	FILE *file;                                        //opening a file stream
    int num = File_count / Thread_count;         
	int tid = (long)thread_id;
	int segment = tid * num;                           // Making segments for processing
	struct dirent *ent; 
	char fileName[256]; 
	char buffer[255]; 
	if ((dir = opendir (Dir_name)) != NULL) {          
		int i = segment + 1;                         
		while ((ent = readdir (dir)) != NULL) {
			if(ent->d_type == DT_REG)                  //Regular file check
		{
			int last_one = tid == Thread_count - 1;
				if (last_one && i > File_count) { // Checking if on last thread.
					break;
				}
			else if (!last_one && i > segment + num) {
					break;
  				}		snprintf(fileName, sizeof fileName, "%s%s\0", Dir_name, ent->d_name); //Redirecting output to a buffer
				file = fopen(fileName, "r");
				if (file == NULL) {
					printf("Unknown file %s\n", fileName);
				}
				else {
					long lines = 0; 
					char *line = NULL;
					char *ip = NULL;
					size_t len = 1000; 
					size_t read;
					while ((read = getline(&line, &len, file)) != -1) {    //Reading file
						lines++;
						ip = strtok (line, " ");                           //Tokenizing string
						pthread_mutex_lock(&mutex_lock);                  // Locking the mutex
						int flag = checkDict(dict, ip); 
						if (flag == 0) { 
							insertDict(dict, ip, "");                      //Adding ip in the dictionary
							ip_count++; 
						}
						pthread_mutex_unlock(&mutex_lock);                // unlocking the mutex after task completed
					}
					fclose(file);
				}
				i++;
		}}}
		
	closedir(dir);
	pthread_exit(NULL); 
}

static unsigned long hash(const char *str)             //Function for hashing
{

	int j;
	unsigned long hash = 6400;
	while (j = *str++) {
	hash = ((hash << 5) + hash) + j;
	}
	return hash;
}

static Pair_variable *get_pair(A_Array *A_array, const char *key)
{
	Pair_variable *pair;
	unsigned int i;
	unsigned int n;
	n = A_array->count;
	if (n == 0) {
		return NULL;
	}
	pair = A_array->pairs;
	i = 0;
	while (i < n) {
		if (pair->key != NULL && pair->value != NULL) {
			if (strcmp(pair->key, key) == 0) {
				return pair;
			}
		}
		pair++;
		i++;
	}
	return NULL;
}


int countFiles(DIR *dir)                                //Function to count the number of files
    {
    struct dirent *ent;                          
	int count = 0; 
	while ((ent = readdir (dir)) != NULL) {
		if(ent->d_type == DT_REG) {
			count++;
		}
	}
	return count;
}


int insertDict(Dict *dict, const char *key, const char *value)
{
	unsigned int index;
	Pair_variable *tmp_pairs, *pair;
	char *tmp_value;
	unsigned int value_len;
	char *new_key, *new_value;
	unsigned int key_len;
	A_Array *A_array;
	if (dict == NULL) {
		return 0;
	}
	if (key == NULL || value == NULL) {
		return 0;
	}
	key_len = strlen(key);
	value_len = strlen(value);
	index = hash(key) % dict->count;
	A_array = &(dict->A_arrays[index]);
	if ((pair = get_pair(A_array, key)) != NULL) {          //Same value to a different key
		if (strlen(pair->value) < value_len) {             //Checking if new value > old value
			tmp_value = realloc(pair->value, (value_len + 1) * sizeof(char));   //Allocating memory
			if (tmp_value == NULL) {
				return 0;
			}
			pair->value = tmp_value;
		}
		strcpy(pair->value, value);                        //Updating with the new value
		return 1;
	}
		new_key = malloc((key_len + 1) * sizeof(char));     //Allocating memory for key
	if (new_key == NULL) {
		return 0;
	}
	new_value = malloc((value_len + 1) * sizeof(char));      //Allocating memory for corresponding value
	if (new_value == NULL) {
		free(new_key);
		return 0;
	}
		if (A_array->count == 0) {
		
		A_array->pairs = malloc(sizeof(Pair_variable));            //Updating A_Array
		if (A_array->pairs == NULL) {
			free(new_key);
			free(new_value);
			return 0;
		}
		A_array->count = 1;
	}
	else {


		tmp_pairs = realloc(A_array->pairs, (A_array->count + 1) * sizeof(Pair_variable));
		if (tmp_pairs == NULL) {
			free(new_key);
			free(new_value);
			return 0;
		}
		A_array->pairs = tmp_pairs;
		A_array->count++;
	}
	pair = &(A_array->pairs[A_array->count - 1]);
	pair->key = new_key;
	pair->value = new_value;
	strcpy(pair->key, key);                                   //Updating
	strcpy(pair->value, value);
	return 1;
}


Dict *createDict(unsigned int capacity)              //Dictionary creation
{
	Dict *dict;
	dict = malloc(sizeof(Dict));                         //Allocating memory
	if (dict == NULL) {
		return NULL;
	}
	dict->count = capacity;
	dict->A_arrays = malloc(dict->count * sizeof(A_Array));
	if (dict->A_arrays == NULL) {
		free(dict);
		return NULL;
	}
	memset(dict->A_arrays, 0, dict->count * sizeof(A_Array));       //Fill memory block
	return dict;
}


