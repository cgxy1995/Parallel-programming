#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct work{
	char str[50];
	int count;
}work;
typedef struct workQ{
   struct work* works;
   int pos;
   int size;
}workQ;
workQ *initQ(int n){
	workQ *queue = (workQ*)malloc(sizeof(workQ));
	queue->works = (work*)malloc(sizeof(work) * n);
	queue->pos = -1;
	queue->size = n - 1;
	return queue;
}
work constr_work(char str[]){
	work word;
	strcpy(word.str,str);
	word.count = 1;
	return word;
}
void putWork(workQ* workQ, work work) {
   if (workQ->pos < (workQ->size)) {
      workQ->pos++;
      printf("putting work %s to %d\n",work.str,workQ->pos);
      workQ->works[workQ->pos] = work;
   } else printf("ERROR: attempt to add Q element%d\n", workQ->pos+1);
}

work getWork(workQ *workQ) {
   if (workQ->pos > -1) {
      work w = workQ->works[workQ->pos];
      printf("getting work %s from %d\n",w.str,workQ->pos);
      workQ->pos--;
      return w;
   } else printf("ERROR: attempt to get work from empty Q%d\n", workQ->pos);
}
int is_empty(workQ *workQ) {
	int r;
	if(workQ->pos == -1)
		return 1;
	else
		return 0;
}
void printQ(workQ *queue){
	int i;
	printf("-----------------------------------\n");
	for(i=0;i<=queue->pos;i++){
		printf("%s [%d] ",queue->works[i].str,queue->works[i].count);
	}
	printf("\n");
}
int queue_contains(workQ *queue, work w){
	int i;
	if(queue->pos == -1)
		return -1;
	for(i=0;i<=queue->pos;i++){
		if(!strcmp(queue->works[i].str, w.str)){
			return i;
		}
	}
	return -1;
}
int mapper(workQ *queue, work w){ //return 1 if queue_to_reduce already contains this work, otherwise returns 0
	int work_pos;
	//printf("mapping %s\n",w.str);
	work_pos = queue_contains(queue, w);
	if(work_pos >= 0){
		queue->works[work_pos].count++;
		return 1;
	}else{
		putWork(queue, w);
		return 0;
	}
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
int main(int argc, char **argv){
	FILE *file = fopen("file1","r");
	FILE *out = NULL;
	char str_buf[1024][50];
	unsigned str_buf_in = 0;
	unsigned str_buf_out = 0;
	char str[50];
	int read_finish = 0;
	int num_read = 0, num_write = 0;
	int mapping_done = 0;
	char **input_filenames = NULL;
	int input_len; //num of input files
	FILE **input_files = NULL;
	int i,j;
	////locks///
	int reader_lock = 0;
	int mapper_lock = 0;
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
	omp_set_num_threads(2);
	workQ *queue_to_map = initQ(100000);
	workQ *queue_to_reduce = initQ(100000);
	//workQ *queue_to_redece2
	#pragma omp parallel sections
	{
	#pragma omp section //reader thread
	{
		int i;
		for(i=0;i<input_len;i++){
			while(!feof(input_files[i])){
                  /////////check if full///////////
				fscanf(input_files[i],"%s",str);
				//printf("reading %s\n",str);
				while(mapper_lock){}
				reader_lock = 1;
				putWork(queue_to_map, constr_work(str));
				reader_lock = 0;
				num_read++;
			}
			fclose(input_files[i]);
		}
		read_finish = 1;
		printf("reader thread done\n");
	}
	/*#pragma omp section //writer thread
	{
		while(!read_finish || (str_buf_in != str_buf_out)){
			while(str_buf_in == str_buf_out){} //wait if buffer empty
			if(fprintf(out,"%s ",str_buf[str_buf_out]) < 0)
				printf("write fails\n");
			str_buf_out++;
		}
		fclose(out);
	}*/
	#pragma omp section //mapper thread 1
	{
		while(!read_finish || !is_empty(queue_to_map)){
			if(!is_empty(queue_to_map)){
				while(reader_lock){}
				mapper_lock = 1;
				work work = getWork(queue_to_map);
				mapper_lock = 0;
				mapper(queue_to_reduce, work);
			}
		}
		printf("mapper thread done\n");
	}
	}
	printQ(queue_to_reduce);
	/*for(i=0;i<dist_count;i++){
		work work = getWork(queue_to_reduce);
		printf("%s, %d\n",work.str,work.count);
	}*/
	return 0;
}