# 编译加了libpmem库的源文件时加上-l pmem参数进行编译
# 在这个makefile中添加ycsb测试的相关编译命令，运行ycsb

CC := g++
INC := ../include
FLAGS := -std=c++11 -w

ycsb:	ycsb.o pm_ehash.o data_page.o
	${CC} ${FLAGS} -o $@ $^ -lpmem 

%.o: %.cpp
	${CC} ${FLAGS} -I ${INC} -o $@ -c $^

clean: 
	@rm *.o
	@rm ycsb