#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <omp.h>
#include <mpi.h>
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
      fifoQ->works[fifoQ->in] = work;
   } else printf("ERROR: attempt to add Q element%d\n", fifoQ->in+1);
   omp_unset_lock(&worklock);
}

work getWork(fifoQ *fifoQ) {
	omp_set_lock(&worklock);
   if (fifoQ->in >= fifoQ->out) {
      work w = fifoQ->works[fifoQ->out];
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
		//printf("mapping %s, found same\n",w.str);
		omp_set_lock(&mapperlock);
		queue->works[work_pos].count++;
		//printf("%s: %d\n", queue->works[work_pos].str,queue->works[work_pos].count);
		omp_unset_lock(&mapperlock);
		return 1;
	}else{
		//printf("mapping %s, found nothing\n",w.str);
		putWork(queue, w);
		//omp_unset_lock(&mapperlock);
		return 0;
	}
}
fifoQ* reducer(fifoQ *to_reduce){
	fifoQ* reduced = initQ(10000,"reduced");
	int i;
	int work_pos;
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
int combine_queue(fifoQ * dest, fifoQ *src){
	int i;
	for(i=0;i<=src->in;i++){
		putWork(dest, src->works[i]);
	}
	return 1;
}
int hash(char *word){
	//printf("hash:%d word %s %d\n",word[0] % 5,word,word[0]);
	return (unsigned)word[0] % 4;
}
int calcnum(fifoQ ** queue,int num){
	int i;
	int sum=0;
	for(i=0;i<num;i++){
		sum+=(queue[i]->in+1);
	}
	return sum;
}
int main(int argc, char **argv){
	FILE *file = fopen("file1","r");
	FILE *out = NULL;
	char str_buf[1024][50];
	unsigned str_buf_in = 0;
	unsigned str_buf_out = 0;
	char str[50];
	int read_finish = 0;
	int num_read = 0, num_write = 0;
	char **input_filenames = NULL;
	int input_len; //num of input files
	FILE **input_files = NULL;
	int i,j; double elapsed_time;
	int mapping_done = 0;//done when all mapper thread done
	struct timeval tvalBefore, tvalAfter;
	////locks///
	int rank, size, len;
    char name[MPI_MAX_PROCESSOR_NAME];
    omp_set_num_threads(4);
    MPI_Init(&argc, &argv);                 
    MPI_Comm_size(MPI_COMM_WORLD, &size);   
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);  
    MPI_Get_processor_name(name, &len);
    MPI_Status status;
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
	omp_set_num_threads(8);
	
	fifoQ *queue_to_map = initQ(1000000, "queue_to_map");
	fifoQ *queue_to_reduce = initQ(1000000, "queue_to_map");
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
	fifoQ *final_queue = initQ(1000000, "final Q");
	
	int sendsize = input_len/size + (input_len % size - rank > 0 ? 1 : 0); //num of files send to a node
	if(rank==0){ //distribute files
		int i,j;
		char ***files_tosend = (char***)malloc(sizeof(char**)*input_len);
		int lsendsize;
		FILE **node_files;
		
		for(i=0;i<size;i++){
			lsendsize = input_len/size + (input_len % size - i > 0 ? 1 : 0); //num of files send to a node
			printf("send size of core %d is %d\n",i,lsendsize);
			files_tosend[i] = (char**)malloc(sizeof(char*)*lsendsize);
			for(j=0;j<lsendsize;j++){
				files_tosend[i][j] = (char*)malloc(sizeof(char)*50);
			}
		}
		for(i=0;i<input_len;i++){
			int belongs_to = i % size;
			int pos = i/size;
			strcpy(files_tosend[belongs_to][pos],input_filenames[i]);
			printf("distributing file %s to files_tosend %d,%d, value %s\n",input_filenames[i],belongs_to,pos,files_tosend[belongs_to][pos]);
		}
		if(size>1){
			for(i=1;i<size;i++){
				lsendsize = input_len/size + (input_len % size - i > 0 ? 1 : 0);
				for(j=0;j<lsendsize;j++){
					printf("sending %s to cpu %d\n",files_tosend[i][j],i);
					MPI_Send(files_tosend[i][j],50,MPI_BYTE,i,1,MPI_COMM_WORLD);
					printf("send done\n");
				}
			}
		}
		node_files = (FILE**)malloc(sizeof(FILE*)*sendsize);
		for(i=0;i<sendsize;i++){
			node_files[i] = fopen(files_tosend[rank][i],"r");
		}
		gettimeofday (&tvalBefore, NULL);
		#pragma omp parallel sections
		{
		
		#pragma omp section //reader thread0
		{
			int i; int odd_even = 0;
			//printf("reader 0 is core #%d\n",rank);
			for(i=0;i<sendsize;i++){
				while(!feof(node_files[i])){
	                  /////////check if full///////////
					omp_set_lock(&readerlock);
					if(!feof(node_files[i])){
						strcpy(str,"");
						fscanf(node_files[i],"%s",str);
					}
					else{
						omp_unset_lock(&readerlock);
						break;
					}
					omp_unset_lock(&readerlock);
					if(strcmp(str,""))
						putWork(queues_to_map[0], constr_work(str));
				}
			}
			omp_set_lock(&inclock);
			read_finish++;
			omp_unset_lock(&inclock);
			//printf("reader thread0 done\n");
		}
		#pragma omp section //reader thread1
		{
			int i; int odd_even = 0;
			//printf("reader 1 is core #%d\n",rank);
			for(i=0;i<sendsize;i++){
				while(!feof(node_files[i])){
	                  /////////check if full///////////
					omp_set_lock(&readerlock);
					if(!feof(node_files[i])){
						strcpy(str,"");
						fscanf(node_files[i],"%s",str);
					}
					else{
						omp_unset_lock(&readerlock);
						break;
					}
					omp_unset_lock(&readerlock);
					if(strcmp(str,""))
						putWork(queues_to_map[1], constr_work(str));
				}
			}
			omp_set_lock(&inclock);
			read_finish++;
			omp_unset_lock(&inclock);
			//printf("reader thread1 done\n");
		}
		#pragma omp section //reader thread2
		{
			int i; int odd_even = 0;
			//printf("reader 2 is core #%d\n",rank);
			for(i=0;i<sendsize;i++){
				while(!feof(node_files[i])){
	                  /////////check if full///////////
					omp_set_lock(&readerlock);
					if(!feof(node_files[i])){
						strcpy(str,"");
						fscanf(node_files[i],"%s",str);
					}
					else{
						omp_unset_lock(&readerlock);
						break;
					}
					omp_unset_lock(&readerlock);
					if(strcmp(str,""))
						putWork(queues_to_map[2], constr_work(str));
				}
			}
			omp_set_lock(&inclock);
			read_finish++;
			omp_unset_lock(&inclock);
			//printf("reader thread2 done\n");
		}
		#pragma omp section //reader thread3
		{
			//	printf("reader 3 is core #%d\n",rank);
			int i; int odd_even = 0;
			for(i=0;i<sendsize;i++){
				while(!feof(node_files[i])){
	                  /////////check if full///////////
					omp_set_lock(&readerlock);
					if(!feof(node_files[i])){
						strcpy(str,"");
						fscanf(node_files[i],"%s",str);
					}
					else{
						omp_unset_lock(&readerlock);
						break;
					}
					omp_unset_lock(&readerlock);
					if(strcmp(str,""))
						putWork(queues_to_map[3], constr_work(str));
				}
			}
			omp_set_lock(&inclock);
			read_finish++;
			omp_unset_lock(&inclock);
			//printf("reader thread3 done %d\n",rank);
		}
		#pragma omp section //mapper thread 0
		{
			int i;
			fifoQ *innerQ = initQ(50000,"innerQ 0");
			while(read_finish<NUM_READ_THREADS || !is_empty(queues_to_map[0])){
				printf("");
				if(!is_empty(queues_to_map[0])){
					work work = getWork(queues_to_map[0]);
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
			//printf("mapper thread0 done %d\n",rank);
			gettimeofday (&tvalAfter, NULL);
    elapsed_time = (float)(tvalAfter.tv_sec - tvalBefore.tv_sec)+((float)(tvalAfter.tv_usec - tvalBefore.tv_usec)/1000000);
    if(rank==0)
		printf("elapsed time = %.2f sec,rank %d in map 0\n",elapsed_time,rank);
		}
		#pragma omp section //mapper thread 1
		{
			int i;
			fifoQ *innerQ = initQ(50000,"innerQ 1");
			while(read_finish<NUM_READ_THREADS || !is_empty(queues_to_map[1])){
				printf("");
				if(!is_empty(queues_to_map[1])){		
					work work = getWork(queues_to_map[1]);				
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
			//printf("mapper thread1 done %d\n",rank);
			gettimeofday (&tvalAfter, NULL);
    elapsed_time = (float)(tvalAfter.tv_sec - tvalBefore.tv_sec)+((float)(tvalAfter.tv_usec - tvalBefore.tv_usec)/1000000);
    if(rank==0)
		printf("elapsed time = %.2f sec,rank %d in map 1\n",elapsed_time,rank);
		}
		#pragma omp section //mapper thread 2
		{
			int i;
			fifoQ *innerQ = initQ(50000,"innerQ 2");
			while(read_finish<NUM_READ_THREADS || !is_empty(queues_to_map[2])){
				printf("");
				if(!is_empty(queues_to_map[2])){		
					work work = getWork(queues_to_map[2]);				
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
			//printf("mapper thread2 done %d\n",rank);
			gettimeofday (&tvalAfter, NULL);
    elapsed_time = (float)(tvalAfter.tv_sec - tvalBefore.tv_sec)+((float)(tvalAfter.tv_usec - tvalBefore.tv_usec)/1000000);
    if(rank==0)
		printf("elapsed time = %.2f sec,rank %d in map 2\n",elapsed_time,rank);
		}
		#pragma omp section //mapper thread 3
		{
			int i;
			fifoQ *innerQ = initQ(50000,"innerQ 2");
			while(read_finish<NUM_READ_THREADS || !is_empty(queues_to_map[3])){
				printf("");
				if(!is_empty(queues_to_map[3])){		
					work work = getWork(queues_to_map[3]);				
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
			//printf("mapper thread3 done %d\n",rank);
			gettimeofday (&tvalAfter, NULL);
    elapsed_time = (float)(tvalAfter.tv_sec - tvalBefore.tv_sec)+((float)(tvalAfter.tv_usec - tvalBefore.tv_usec)/1000000);
    if(rank==0)
		printf("elapsed time = %.2f sec,rank %d in map 3\n",elapsed_time,rank);
		}
		#pragma omp section //reducer thread 0 
		{
			int i;
			gettimeofday (&tvalAfter, NULL);
    elapsed_time = (float)(tvalAfter.tv_sec - tvalBefore.tv_sec)+((float)(tvalAfter.tv_usec - tvalBefore.tv_usec)/1000000);
    if(rank==0)
		printf("elapsed time = %.2f sec,rank %d\n",elapsed_time,rank);
			while(mapping_done<NUM_READ_THREADS){
				printf("");
			}
			gettimeofday (&tvalAfter, NULL);
    elapsed_time = (float)(tvalAfter.tv_sec - tvalBefore.tv_sec)+((float)(tvalAfter.tv_usec - tvalBefore.tv_usec)/1000000);
    if(rank==0)
		printf("elapsed time = %.2f sec,rank %d\n",elapsed_time,rank);
			queues_reduced[0] = reducer(queues_to_reduce[0]);
			//printf("reducer thread 0 done\n");
			gettimeofday (&tvalAfter, NULL);
    elapsed_time = (float)(tvalAfter.tv_sec - tvalBefore.tv_sec)+((float)(tvalAfter.tv_usec - tvalBefore.tv_usec)/1000000);
    if(rank==0)
		printf("elapsed time = %.2f sec,rank %d\n",elapsed_time,rank);
		}
		#pragma omp section //reducer thread 1
		{
			int i;
			while(mapping_done<NUM_READ_THREADS){printf("");}
			queues_reduced[1] = reducer(queues_to_reduce[1]);
			//printf("reducer thread 1 done\n");
		}
		#pragma omp section //reducer thread 2 
		{
			int i;
			while(mapping_done<NUM_READ_THREADS){printf("");}
			queues_reduced[2] = reducer(queues_to_reduce[2]);
			//printf("reducer thread 2 done\n");
		}
		#pragma omp section //reducer thread 3
		{
			int i;
			while(mapping_done<NUM_READ_THREADS){printf("");}
			queues_reduced[3] = reducer(queues_to_reduce[3]);
			//printf("reducer thread 3 done\n");
		}
		}
		gettimeofday (&tvalAfter, NULL);
    elapsed_time = (float)(tvalAfter.tv_sec - tvalBefore.tv_sec)+((float)(tvalAfter.tv_usec - tvalBefore.tv_usec)/1000000);
    if(rank==0)
		printf("elapsed time = %.2f sec,rank %d\n",elapsed_time,rank);
	}
	else{
		int i;
		FILE** node_files = (FILE**)malloc(sizeof(FILE*)*sendsize);
		for(i=0;i<sendsize;i++){
			char *bufstr = (char*)malloc(sizeof(char)*50);
			MPI_Recv(bufstr,50,MPI_BYTE, 0,1, MPI_COMM_WORLD, &status);
			//printf("%s received\n",bufstr);
			node_files[i] = fopen(bufstr,"r");
		}
		#pragma omp parallel sections shared(input_files) private(str)
		{	
		//printf("using %d threads in core %d\n",omp_get_num_threads(),rank);
		#pragma omp section //reader thread0
		{
			int i; int odd_even = 0;
		//	printf("reader 0 is core #%d\n",rank);
			for(i=0;i<sendsize;i++){
				while(!feof(node_files[i])){
	                  /////////check if full///////////
					omp_set_lock(&readerlock);
					if(!feof(node_files[i])){
						strcpy(str,"");
						fscanf(node_files[i],"%s",str);
					}
					else{
						omp_unset_lock(&readerlock);
						break;
					}
					omp_unset_lock(&readerlock);
					if(strcmp(str,""))
						putWork(queues_to_map[0], constr_work(str));
				}
			}
			omp_set_lock(&inclock);
			read_finish++;
			omp_unset_lock(&inclock);
			//printf("reader thread0 done\n");
		}
		#pragma omp section //reader thread1
		{
			int i; int odd_even = 0;
		//	printf("reader 1 is core #%d\n",rank);
			for(i=0;i<sendsize;i++){
				while(!feof(node_files[i])){
	                  /////////check if full///////////
					omp_set_lock(&readerlock);
					if(!feof(node_files[i])){
						strcpy(str,"");
						fscanf(node_files[i],"%s",str);
					}
					else{
						omp_unset_lock(&readerlock);
						break;
					}
					omp_unset_lock(&readerlock);
					if(strcmp(str,""))
						putWork(queues_to_map[1], constr_work(str));
				}
			}
			omp_set_lock(&inclock);
			read_finish++;
			omp_unset_lock(&inclock);
			//printf("reader thread1 done\n");
		}
		#pragma omp section //reader thread2
		{
			int i; int odd_even = 0;
			//printf("reader 2 is core #%d\n",rank);
			for(i=0;i<sendsize;i++){
				while(!feof(node_files[i])){
	                  /////////check if full///////////
					omp_set_lock(&readerlock);
					if(!feof(node_files[i])){
						strcpy(str,"");
						fscanf(node_files[i],"%s",str);
					}
					else{
						omp_unset_lock(&readerlock);
						break;
					}
					omp_unset_lock(&readerlock);
					if(strcmp(str,""))
						putWork(queues_to_map[2], constr_work(str));
				}
			}
			omp_set_lock(&inclock);
			read_finish++;
			omp_unset_lock(&inclock);
			//printf("reader thread2 done\n");
		}
		#pragma omp section //reader thread3
		{
				//printf("reader 3 is core #%d\n",rank);
			int i; int odd_even = 0;
			for(i=0;i<sendsize;i++){
				while(!feof(node_files[i])){
	                  /////////check if full///////////
					omp_set_lock(&readerlock);
					if(!feof(node_files[i])){
						strcpy(str,"");
						fscanf(node_files[i],"%s",str);
					}
					else{
						omp_unset_lock(&readerlock);
						break;
					}
					omp_unset_lock(&readerlock);
					if(strcmp(str,""))
						putWork(queues_to_map[3], constr_work(str));
				}
			}
			omp_set_lock(&inclock);
			read_finish++;
			omp_unset_lock(&inclock);
			//printf("reader thread3 done %d\n",rank);
		}
		#pragma omp section //mapper thread 0
		{
		int i;
		fifoQ *innerQ = initQ(50000,"innerQ 0");
		//printf("map1\n");
		while(read_finish<NUM_READ_THREADS || !is_empty(queues_to_map[0])){
			printf("");
			if(!is_empty(queues_to_map[0])){
				work work = getWork(queues_to_map[0]);
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
		//printf("mapper thread0 done %d\n",rank);
		}
		#pragma omp section //mapper thread 1
		{
			int i;
			fifoQ *innerQ = initQ(50000,"innerQ 1");
			while(read_finish<NUM_READ_THREADS || !is_empty(queues_to_map[1])){
				printf("");
				if(!is_empty(queues_to_map[1])){		
					work work = getWork(queues_to_map[1]);				
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
			//printf("mapper thread1 done %d\n",rank);
		}
		#pragma omp section //mapper thread 2
		{
			int i;
			fifoQ *innerQ = initQ(50000,"innerQ 2");
			while(read_finish<NUM_READ_THREADS || !is_empty(queues_to_map[2])){
				printf("");
				if(!is_empty(queues_to_map[2])){		
					work work = getWork(queues_to_map[2]);				
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
			//printf("mapper thread2 done %d\n",rank);
		}
		#pragma omp section //mapper thread 3
		{
			int i;
			fifoQ *innerQ = initQ(50000,"innerQ 2");
			while(read_finish<NUM_READ_THREADS || !is_empty(queues_to_map[3])){
				printf("");
				if(!is_empty(queues_to_map[3])){			
					work work = getWork(queues_to_map[3]);				
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
			//printf("mapper thread3 done %d\n",rank);
		}
		#pragma omp section //reducer thread 0 
		{
			int i;
			while(mapping_done<NUM_READ_THREADS){
				printf("");
			}
			queues_reduced[0] = reducer(queues_to_reduce[0]);
			//printf("reducer thread 0 done\n");
		}
		#pragma omp section //reducer thread 1
		{
			int i;
			while(mapping_done<NUM_READ_THREADS){printf("");}
			queues_reduced[1] = reducer(queues_to_reduce[1]);
			//printf("reducer thread 1 done\n");
		}
		#pragma omp section //reducer thread 2 
		{
			int i;
			while(mapping_done<NUM_READ_THREADS){printf("");}
			queues_reduced[2] = reducer(queues_to_reduce[2]);
			//printf("reducer thread 2 done\n");
		}
		#pragma omp section //reducer thread 3
		{
			int i;
			while(mapping_done<NUM_READ_THREADS){printf("");}
			queues_reduced[3] = reducer(queues_to_reduce[3]);
			//printf("reducer thread 3 done\n");
		}
		
		}
	}
	MPI_Barrier(MPI_COMM_WORLD);
	gettimeofday (&tvalAfter, NULL);
    elapsed_time = (float)(tvalAfter.tv_sec - tvalBefore.tv_sec)+((float)(tvalAfter.tv_usec - tvalBefore.tv_usec)/1000000);
    if(rank==0)
		printf("elapsed time = %.2f sec,rank %d\n",elapsed_time,rank);
	if(rank==0){ //final reducuction
		int i,j,revbuf;int mainct;
		for(i=0;i<NUM_READ_THREADS;i++){
			combine_queue(final_queue,queues_reduced[i]);
		}
		//printf("main node has %d to final reduce\n",calcnum(queues_reduced,NUM_READ_THREADS));
		for(i=1;i<size;i++){
			MPI_Recv(&revbuf,1,MPI_INT,i,1,MPI_COMM_WORLD,&status);
			//printf("need to receive %d strings from node %d\n",revbuf,i);
			char *strbuf = (char*)malloc(sizeof(char)*50);
			char ctbuf = 0;
			for(j=0;j<revbuf;j++){
				MPI_Recv(strbuf,50,MPI_BYTE,i,1,MPI_COMM_WORLD,&status);
				MPI_Recv(&ctbuf,50,MPI_INT,i,1,MPI_COMM_WORLD,&status);
				work work;
				strcpy(work.str,strbuf);
				work.count = ctbuf;
				//printf("received <%s,%d> from node %d\n",work.str,work.count,i);
				putWork(final_queue,work);
			}
		}
		fifoQ *output = reducer(final_queue);
		printQ_to_file(&output,1,out);
	}else{
		int i,total_num;
		total_num = calcnum(queues_reduced,NUM_READ_THREADS);
		MPI_Send(&total_num,1,MPI_INT,0,1,MPI_COMM_WORLD);
		for(i=0;i<NUM_READ_THREADS;i++){
			combine_queue(final_queue,queues_reduced[i]);
		}
		for(i=0;i<total_num;i++){
			MPI_Send(&final_queue->works[i].str,50,MPI_BYTE,0,1,MPI_COMM_WORLD);
			MPI_Send(&final_queue->works[i].count,1,MPI_INT,0,1,MPI_COMM_WORLD);
		}
	}
	
	for(i=0;i<input_len;i++){
		fclose(input_files[i]);
	}
	fclose(out);
	/*printQ(queues_to_map[0]);
	printQ(queues_to_map[1]);
	printQ(queues_to_map[2]);
	printQ(queues_to_map[3]);*/
	/*printQ(queues_reduced[0]);
	printQ(queues_reduced[1]);
	printQ(queues_reduced[2]);
	printQ(queues_reduced[3]);*/
	omp_destroy_lock(&inclock);
	omp_destroy_lock(&worklock);
	omp_destroy_lock(&readlock);
	omp_destroy_lock(&readerlock);
	omp_destroy_lock(&mapperlock);
	MPI_Finalize();

	return 0;
}

