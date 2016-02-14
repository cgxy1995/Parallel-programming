#include <stdio.h>
#include <stdlib.h>
#include <string.h>
int main(){
	FILE* fp = fopen("file1","r");
	#pragma omp parallel sections
	{
	#pragma omp section
	{
		char str[50];
		while(!feof(fp)){
			fscanf(fp,"%s",str);
			printf("%s <",str);
		}
	}
	#pragma omp section
	{
		char str[50];
		while(!feof(fp)){
			fscanf(fp,"%s",str);
			printf("%s >",str);
		}
	}
	}
	fclose(fp);
}