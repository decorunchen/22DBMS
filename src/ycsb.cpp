#include "pm_ehash.h"
#include <fstream>
#include <iostream>
#include <dirent.h>
#include <regex>
#include <vector>
#include <string.h>
#include <cstdio>
#include <time.h>

#define WORKLOAD "../workloads/"

using namespace std;

const regex NameReg("(\\d+)w-rw-(\\d+)-(\\d+)-load.txt"); // load.txt 的正则匹配                           

typedef struct file{
  string name;
  string type;          //类型
  uint32_t read;         //读比例
  uint32_t write;        //写比例
  uint32_t total_num;    //总操作数
}file;

vector<file> files;       // 储存文件相关信息

/**
 * @description: 加载目录中文件大致信息
 * @param NULL
 * @return: NULL
 */
void LoadFileCatalog()
{   
  files.clear();
  file file;
  string name;
  DIR *dp;
  struct dirent *dirp;
  int x;
  if ((dp = opendir(WORKLOAD)) == NULL) // 打开文件夹失败
  {
    cout << "Can't open " << WORKLOAD << endl;
  }
  while ((dirp = readdir(dp)) != NULL) // 打开文件夹
  {
    if(regex_match(dirp->d_name, NameReg)){             //加载文件信息
      name = dirp->d_name;
      name = name.substr(0,name.length() - 8);
      file.name = name;
      file.total_num = atoi(name.substr(0, name.find("-", 0) - 1).c_str());
      name = name.substr(name.find("-", 0) + 1);       
      file.type = name.substr(0, name.find("-", 0));
      name = name.substr(name.find("-", 0) + 1);
      file.read = atoi(name.substr(0,name.find("-", 0)).c_str());
      file.write = 100 - file.read;
      files.push_back(file);
    }
  }
  closedir(dp);
}

void LoadFile(file loadedfile, PmEHash *db){
  fstream f;
  string operate, num;
  time_t end, begin = clock(); 
  kv kv_pair;
  long long data_num = 0; // 计算数据数量
  f.open(WORKLOAD + loadedfile.name + "load.txt", ios::in);

  cout << "[START], Start to Load " << loadedfile.name + "load.txt" << endl;
  cout << "---------------------------------------------------" << endl;
  while (f.peek() != EOF)
  {
    operate = "";          //清空字符 
    f >> operate;
    if (operate == "")
      break;
    f >> num;
    kv_pair.key = atoi(num.substr(0, 8).c_str());
    kv_pair.value = atoi(num.substr(8).c_str());

    db->insert(kv_pair); //插入数据
    data_num++;
    if (data_num % 10000 == 0) //每1W次插入输出一次
      cout << "[LOADING], Load " << data_num << "/" << loadedfile.total_num * 10000 << endl;
  }
  end = clock(); 
  cout << endl << "[FINISH], Finish "<< loadedfile.name + "load.txt" << endl;
  printf("[OVERALL], RunTims(s), %.5lf\n\n\n", ((double)end - begin) / CLOCKS_PER_SEC); // 输出操作时间
  f.close();
}

uint64_t stringTo64(string str);

void RunFile(file loadedfile, PmEHash *db)
{
  fstream f;
  kv kv_pair;
  string operate,num;
  time_t end, begin; // 计时开始
  long long data_num = 0;   // 记录操作数
  long long insert = 0;     // 记录插入操作数
  long long update = 0;     // 记录更新操作数
  long long read = 0;     // 记录查找操作数
  long long del = 0;    // 记录删除操作数

  f.open(WORKLOAD + loadedfile.name + "run.txt", ios::in);
  // 开始标志栏
  cout << "[START], Start to Run " << loadedfile.name + "run.txt" << endl;
  cout << "---------------------------------------------------" << endl;
  begin = clock();      //运行开始
  while (f.peek() != EOF)
  {
    operate = "";          //清空字符          
    f >> operate;
    if (operate == "")
      break;
    f >> num;

    kv_pair.key = atoi(num.substr(0, 8).c_str());
    kv_pair.value = atoi(num.substr(8).c_str());

    uint64_t value = 0;
    if(operate == "READ"){
      db->search(kv_pair.key, value);
      read++;
    }else if(operate == "INSERT"){
      db->insert(kv_pair);
      insert++;
    }else if(operate == "UPDATE"){
      db->update(kv_pair);
      update++;
    }else{
      db->remove(kv_pair.key);
      del++;
    }
    data_num++;
    if (data_num % 10000 == 0)
      cout << "[RUN], Running " << data_num << " command" << endl;
  }
  end = clock(); // 运行结束

  double spentTime = ((double)end - begin) / CLOCKS_PER_SEC;
  cout << "---------------------------------------------------" << endl;
  cout << "[FINISH] Run " << loadedfile.name + "run.txt" <<" finished "<< endl;
  cout << "---------------------------------------------------" << endl;
  printf("[INSERT], Operations, %lld\n", insert); // 输出插入操作数
  printf("[READ], Operations, %lld\n", read); // 输出查找操作数
  printf("[UPDATE], Operations, %lld\n", update); // 输出更新操作数
  printf("[DELETE], Operations, %lld\n", del); // 输出删除操作数
  printf("[ALL], Operations, %lld\n", data_num); // 输出总操作数
  printf("[OVERALL], RunTims(s), %.5lf\n", spentTime); // 输出操作时间
  printf("[OVERALL], Throughput(ops/sec), %.5lf\n\n\n", (long)(data_num / spentTime)); // 输出每秒操作数
  f.close();
}

int main(){
  PmEHash *db;
  LoadFileCatalog();
  for (int i = 0; i < files.size(); i++)
   {
     db = new PmEHash();
     LoadFile(files[i], db);
     RunFile(files[i], db);
     db->selfDestory();
   }
   return 0;
}