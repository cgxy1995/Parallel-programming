#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <omp.h>
#define NUM_READ_THREADS 4
typedef struct work{
	char str[50];
	int count;
}work;
typedef struct fifoQ{
	struct work* works;
	int in,out;
	int size;
	char name[50];
}fifoQ;
omp_lock_t worklock;
omp_lock_t inclock;
omp_lock_t readerlock;
omp_lock_t readlock;
omp_lock_t mapperlock;
int test_set(int *lock){
	if(*lock == 0){
		*lock = 1;
		return 0;
	}else{
		return 1;
	}
}
int total_words;
void lock_aquire(int *lock){
	while(test_set(lock));
}
void lock_release(int *lock){
	*lock = 0;
}
fifoQ *initQ(int n, char *name){
	fifoQ *fifo = (fifoQ*)malloc(sizeof(fifoQ));
	fifo->works = (work*)malloc(sizeof(work) * n);
	fifo->in = -1;
	fifo->out = 0;
	strcpy(fifo->name, name);
	fifo->size = n - 1;
	return fifo;
}
work constr_work(char str[]){
	work word;
	strcpy(word.str,str);
	word.count = 1;
	return word;
}
void putWork(fifoQ* fifoQ, work work) {
    omp_set_lock(&worklock);
   if (fifoQ->in < (fifoQ->size)) {
      fifoQ->in++;
      //printf("%d %dputting work %s to %s pos %d\n",fifoQ->in,fifoQ->out,work.str,fifoQ->name, fifoQ->in);
      fifoQ->works[fifoQ->in] = work;
   } else printf("ERROR: attempt to add Q element%d\n", fifoQ->in+1);
   omp_unset_lock(&worklock);
}

work getWork(fifoQ *fifoQ) {
	omp_set_lock(&worklock);
   if (fifoQ->in >= fifoQ->out) {
      work w = fifoQ->works[fifoQ->out];
      //printf("%d %dgetting work %s from %s pos %d\n",fifoQ->in,fifoQ->out,w.str,fifoQ->name, fifoQ->out);
      fifoQ->out++;
      omp_unset_lock(&worklock);
      return w;
   } else printf("ERROR: attempt to get work from empty Q%d\n", fifoQ->out+1);
   omp_unset_lock(&worklock);
}
int is_empty(fifoQ *fifoQ) {
	return fifoQ->in >= fifoQ->out ? 0:1;
}
void printQ(fifoQ *queue){
	int i;
	printf("%d-----------------------------------%d\n",queue->out,queue->in);
	for(i=0;i<=queue->in;i++){
		printf("%s [%d] ",queue->works[i].str,queue->works[i].count);
	}
	printf("\n");
}
void printQ_to_file(fifoQ **queues, int size, FILE* file){
	int i,j;
	for(i=0;i<size;i++){
		for(j=0;j<=queues[i]->in;j++)
			fprintf(file,"<%s,%d>\n",queues[i]->works[j].str,queues[i]->works[j].count);
	}
}
int queue_contains(fifoQ *queue, work w){
	int i;
	if(queue->in == -1)
		return -1;
	for(i=0;i<=queue->in;i++){
		if(!strcmp(queue->works[i].str, w.str)){
			return i;
		}
	}
	return -1;
}
int mapper(fifoQ *queue, work w){ //return 1 if queue_to_reduce already contains this work, otherwise returns 0
	int work_pos;
	
	work_pos = queue_contains(queue, w);
	if(work_pos >= 0){
		omp_set_lock(&mapperlock);
		queue->works[work_pos].count++;
		omp_unset_lock(&mapperlock);
		return 1;
	}else{
		putWork(queue, w);
		return 0;
	}
}
fifoQ* reducer(fifoQ *to_reduce){
	fifoQ* reduced = initQ(10000,"reduced");
	int i,work_pos;
	for(i=0;i<=to_reduce->in;i++){
		work_pos = queue_contains(reduced, to_reduce->works[i]);
		if(work_pos >= 0){
			reduced->works[work_pos].count += to_reduce->works[i].count;
		}else{
			putWork(reduced, to_reduce->works[i]);
		}
	}
	return reduced;
}
FILE **read_in(char **filenames, int num, int mode){ //mode 0=read,1=write
	FILE **files = (FILE**)malloc(sizeof(FILE*)*num);
	int i;
	if(mode == 0){
		for(i=0;i<num;i++){
			//printf("reading %s\n",filenames[i]);
			files[i] = fopen(filenames[i], "r");
		}
	}else if(mode == 1){
		for(i=0;i<num;i++){
			files[i] = fopen(filenames[i], "w");
		}
	}else{
		printf("write file open mode\n");
		return NULL;
	}
	return files;
}
int hash(char *word){
	//printf("hash:%d word %s %d\n",word[0] % 5,word,word[0]);
	return (unsigned)word[0] % 5;
}
int main(int argc, char **argv){
	FILE *file = fopen("file1","r");
	FILE *out = NULL;
	char str_buf[1024][50];
	unsigned str_buf_in = 0;
	unsigned str_buf_out = 0;
	char str[50];
	total_words = 0;
	int read_finish = 0;
	int num_read = 0, num_write = 0;
	char **input_filenames = NULL;
	int input_len; //num of input files
	FILE **input_files = NULL;
	int i,j; double elapsed_time;
	struct timeval tvalBefore, tvalAfter;
	////locks///
	int reader_lock = 0;
	int mapper_lock1 = 1; int mapper_lock3 = 0; int mapper_lock4 = 0; int mapper_lock5 = 0;
	int mapper_lock2 = 0; int reducer_lock1 = 1;
	int mapping_done = 0;//done when all mapper thread done
	omp_init_lock(&worklock);
	omp_init_lock(&inclock);
	omp_init_lock(&readlock);
	omp_init_lock(&readerlock);
	omp_init_lock(&mapperlock);
	if(argc < 5){
		printf("Usage ./mapreduce -in [input files].... -out [output file]\n");
		return 0;
	}else{
		if(strcmp("-in",argv[1])){
			printf("Usage ./mapreduce -in [input files].... -out [output file]\n");
			return 0;
		}
		for(i=2;i<argc;i++){ //start from first input file
			if(!strcmp("-out",argv[i])){
				break;
			}
		}
		input_len = i - 2;
		input_filenames = (char**)malloc(sizeof(char*)*input_len);
		for(j=0;j<input_len;j++)
			input_filenames[j] = (char*)malloc(sizeof(char)*50);
		for(i=2,j=0;j<input_len;i++,j++){
			strcpy(input_filenames[j],argv[i]);
		}
		input_files = read_in(input_filenames,input_len,0);
		if(strcmp("-out",argv[2+input_len])){
			printf("output file missing, using default name 'out'\n");
			out = fopen("out","w");
		}else{
			out = fopen(argv[3+input_len],"w");
		}
	}
	omp_set_num_threads(15);
	fifoQ **queues_to_map = (fifoQ**)malloc(sizeof(fifoQ*)*5);
	queues_to_map[0] = initQ(1000000, "queue_to_map0");
	queues_to_map[1] = initQ(1000000, "queue_to_map1");
	queues_to_map[2] = initQ(1000000, "queue_to_map2");
	queues_to_map[3] = initQ(1000000, "queue_to_map3");
	queues_to_map[4] = initQ(1000000, "queue_to_map4");
	fifoQ **queues_to_reduce = (fifoQ**)malloc(sizeof(fifoQ*)*5);
	queues_to_reduce[0] = initQ(1000000, "queue_to_reduce0");
	queues_to_reduce[1] = initQ(1000000, "queue_to_reduce1");
	queues_to_reduce[2] = initQ(1000000, "queue_to_reduce2");
	queues_to_reduce[3] = initQ(1000000, "queue_to_reduce3");
	queues_to_reduce[4] = initQ(1000000, "queue_to_reduce4");
	fifoQ **queues_reduced = (fifoQ**)malloc(sizeof(fifoQ*)*5);
	//queues_reduced[0] = initQ(100000, "queue_reduced0");
	//queues_reduced[1] = initQ(100000, "queue_reduced1");
	//workQ *queue_to_redece2
	gettimeofday (&tvalBefore, NULL);
	#pragma omp parallel sections shared(input_files) private(str)
	{
	#pragma omp section //reader thread0
	{
		int i; int odd_even = 0;
		for(i=0;i<input_len;i++){
			while(!feof(input_files[i])){
                  /////////check if full///////////
				omp_set_lock(&readerlock);
				if(!feof(input_files[i])){
					fscanf(input_files[i],"%s",str);
					++total_words;
				}
				else{
					omp_unset_lock(&readerlock);
					break;
				}
				omp_unset_lock(&readerlock);
				putWork(queues_to_map[0], constr_work(str));
			}
		}
		omp_set_lock(&inclock);
		read_finish++;
		omp_unset_lock(&inclock);
		printf("reader thread0 done\n");
	}
	#pragma omp section //reader thread1
	{
		int i; int odd_even = 0;
		for(i=0;i<input_len;i++){
			while(!feof(input_files[i])){
                  /////////check if full///////////
				omp_set_lock(&readerlock);
				if(!feof(input_files[i])){
					fscanf(input_files[i],"%s",str);
					++total_words;
				}
				else{
					omp_unset_lock(&readerlock);
					break;
				}
				omp_unset_lock(&readerlock);
				//while(mapper_lock2){}
				//reader_lock = 1;
				putWork(queues_to_map[1], constr_work(str));
				//str = NULL;
				//reader_lock = 0;
			}
		}
		omp_set_lock(&inclock);
		read_finish++;
		omp_unset_lock(&inclock);
		printf("reader thread1 done\n");
	}
	#pragma omp section //reader thread2
	{
		int i; int odd_even = 0;
		for(i=0;i<input_len;i++){
			while(!feof(input_files[i])){
                  /////////check if full///////////
				omp_set_lock(&readerlock);
				if(!feof(input_files[i])){
					fscanf(input_files[i],"%s",str);
					++total_words;
				}
				else{
					omp_unset_lock(&readerlock);
					break;
				}
				omp_unset_lock(&readerlock);
				//while(mapper_lock3){}
				//reader_lock = 1;
				putWork(queues_to_map[2], constr_work(str));
				//reader_lock = 0;
			}
		}
		omp_set_lock(&inclock);
		read_finish++;
		omp_unset_lock(&inclock);
		printf("reader thread2 done\n");
	}
	#pragma omp section //reader thread3
	{
		int i; int odd_even = 0;
		for(i=0;i<input_len;i++){
			while(!feof(input_files[i])){
                  /////////check if full///////////
				omp_set_lock(&readerlock);
				if(!feof(input_files[i])){
					fscanf(input_files[i],"%s",str);
					++total_words;
				}
				else{
					omp_unset_lock(&readerlock);
					break;
				}
				omp_unset_lock(&readerlock);
				//while(mapper_lock4){}
				//reader_lock = 1;
				putWork(queues_to_map[3], constr_work(str));
				//reader_lock = 0;
			}
		}
		omp_set_lock(&inclock);
		read_finish++;
		omp_unset_lock(&inclock);
		printf("reader thread3 done\n");
	}
	/*#pragma omp section //reader thread4
	{
		int i; int odd_even = 0;
		for(i=0;i<input_len;i++){
			while(!feof(input_files[i])){
                  /////////check if full///////////
				omp_set_lock(&readerlock);
				if(!feof(input_files[i])){
					fscanf(input_files[i],"%s",str);
					++total_words;
				}
				else{
					omp_unset_lock(&readerlock);
					break;
				}
				omp_unset_lock(&readerlock);
				//while(mapper_lock5){}
				//reader_lock = 1;
				putWork(queues_to_map[4], constr_work(str));
				//reader_lock = 0;
			}
		}
		omp_set_lock(&inclock);
		read_finish++;
		omp_unset_lock(&inclock);
		printf("reader thread4 done\n");
	}*/
	#pragma omp section //mapper thread 0
	{
		int i;
		fifoQ *innerQ = initQ(50000,"innerQ 0");
		printf("map1\n");
		while(read_finish<NUM_READ_THREADS || !is_empty(queues_to_map[0])){
			while(reader_lock){}
			mapper_lock1 = 1;
			if(!is_empty(queues_to_map[0])){
				work work = getWork(queues_to_map[0]);
				//mapper(queues_to_reduce[hash(work.str)], work);
				mapper(innerQ, work);
			}
			mapper_lock1 = 0;
		}
		for(i=0;i<=innerQ->in;i++){
			work work = getWork(innerQ);
			putWork(queues_to_reduce[hash(work.str)],work);
		}
		omp_set_lock(&inclock);
		mapping_done++;
		omp_unset_lock(&inclock);
		printf("mapper thread0 done %d %d %d\n",is_empty(queues_to_map[0]),queues_to_map[0]->in,queues_to_map[0]->out);
	}
	#pragma omp section //mapper thread 1
	{
		int i;
		fifoQ *innerQ = initQ(50000,"innerQ 1");
		while(read_finish<NUM_READ_THREADS || !is_empty(queues_to_map[1])){
			while(reader_lock){}
			if(!is_empty(queues_to_map[1])){
				mapper_lock2 = 1;			
				work work = getWork(queues_to_map[1]);				
				//mapper(queues_to_reduce[hash(work.str)], work);
				mapper(innerQ, work);
			}
			mapper_lock2 = 0;
		}
		for(i=0;i<=innerQ->in;i++){
			work work = getWork(innerQ);
			putWork(queues_to_reduce[hash(work.str)],work);
		}
		omp_set_lock(&inclock);
		mapping_done++;
		omp_unset_lock(&inclock);
		printf("mapper thread1 done\n");
	}
	#pragma omp section //mapper thread 2
	{
		int i;
		fifoQ *innerQ = initQ(50000,"innerQ 2");
		while(read_finish<NUM_READ_THREADS || !is_empty(queues_to_map[2])){
			while(reader_lock){}
			if(!is_empty(queues_to_map[2])){
				mapper_lock3 = 1;			
				work work = getWork(queues_to_map[2]);				
				//mapper(queues_to_reduce[hash(work.str)], work);
				mapper(innerQ, work);
			}
			mapper_lock3 = 0;
		}
		for(i=0;i<=innerQ->in;i++){
			work work = getWork(innerQ);
			putWork(queues_to_reduce[hash(work.str)],work);
		}
		omp_set_lock(&inclock);
		mapping_done++;
		omp_unset_lock(&inclock);
		printf("mapper thread2 done\n");
	}
	#pragma omp section //mapper thread 3
	{
		int i;
		fifoQ *innerQ = initQ(50000,"innerQ 2");
		while(read_finish<NUM_READ_THREADS || !is_empty(queues_to_map[3])){
			while(reader_lock){}
			if(!is_empty(queues_to_map[3])){
				mapper_lock4 = 1;			
				work work = getWork(queues_to_map[3]);				
				//mapper(queues_to_reduce[hash(work.str)], work);
				mapper(innerQ, work);
			}
			mapper_lock4 = 0;
		}
		for(i=0;i<=innerQ->in;i++){
			work work = getWork(innerQ);
			putWork(queues_to_reduce[hash(work.str)],work);
		}
		omp_set_lock(&inclock);
		mapping_done++;
		omp_unset_lock(&inclock);
		printf("mapper thread3 done\n");
	}
	/*#pragma omp section //mapper thread 4
	{
		int i;
		fifoQ *innerQ = initQ(50000,"innerQ 2");
		while(read_finish<NUM_READ_THREADS || !is_empty(queues_to_map[4])){
			while(reader_lock){}
			if(!is_empty(queues_to_map[4])){
				mapper_lock5 = 1;			
				work work = getWork(queues_to_map[4]);				
				//mapper(queues_to_reduce[hash(work.str)], work);
				mapper(innerQ, work);
			}
		}
		for(i=0;i<=innerQ->in;i++){
			work work = getWork(innerQ);
			putWork(queues_to_reduce[hash(work.str)],work);
		}
		omp_set_lock(&inclock);
		mapping_done++;
		omp_unset_lock(&inclock);
		printf("mapper thread4 done\n");
	}*/
	#pragma omp section //reducer thread 0 
	{
		int i;
		while(mapping_done<NUM_READ_THREADS){
			//printf("%d \n",mapping_done);
		}
		queues_reduced[0] = reducer(queues_to_reduce[0]);
		printf("reducer thread 0 done\n");
	}
	#pragma omp section //reducer thread 1
	{
		int i;
		while(mapping_done<NUM_READ_THREADS){}
		queues_reduced[1] = reducer(queues_to_reduce[1]);
		printf("reducer thread 1 done\n");
	}
	#pragma omp section //reducer thread 2 
	{
		int i;
		while(mapping_done<NUM_READ_THREADS){}
		queues_reduced[2] = reducer(queues_to_reduce[2]);
		printf("reducer thread 2 done\n");
	}
	#pragma omp section //reducer thread 3
	{
		int i;
		while(mapping_done<NUM_READ_THREADS){}
		queues_reduced[3] = reducer(queues_to_reduce[3]);
		printf("reducer thread 3 done\n");
	}
	#pragma omp section //reducer thread 4
	{
		int i;
		while(mapping_done<NUM_READ_THREADS){}
		queues_reduced[4] = reducer(queues_to_reduce[4]);
		printf("reducer thread 4 done\n");
	}
	}
	printf("allthreads done\n");
	gettimeofday (&tvalAfter, NULL);
    elapsed_time = (float)(tvalAfter.tv_sec - tvalBefore.tv_sec)+((float)(tvalAfter.tv_usec - tvalBefore.tv_usec)/1000000);
	printQ_to_file(queues_reduced,4,out);
	printf("elapsed time = %.2f sec\n",elapsed_time);
	for(i=0;i<input_len;i++){
		fclose(input_files[i]);
	}
	fclose(out);
	omp_destroy_lock(&inclock);
	omp_destroy_lock(&worklock);
	omp_destroy_lock(&readlock);
	omp_destroy_lock(&readerlock);
	omp_destroy_lock(&mapperlock);
	/*printQ(queues_to_map[0]);
	printQ(queues_to_map[1]);
	printQ(queues_to_map[2]);
	printQ(queues_to_map[3]);
	printQ(queues_to_map[4]);
	printQ(queues_reduced[0]);
	printQ(queues_reduced[1]);
	printQ(queues_reduced[2]);
	printQ(queues_reduced[3]);
	printQ(queues_reduced[4]);*/
	/*for(i=0;i<dist_count;i++){
		work work = getWork(queue_to_reduce);
		printf("%s, %d\n",work.str,work.count);
	}*/
	printf("Total words of the essay is: %d\n", total_words);
	return 0;
}