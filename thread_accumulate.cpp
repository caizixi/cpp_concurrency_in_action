#include<iostream>
#include<thread>
#include<numeric>
#include<algorithm>
#include<functional>
#include<vector>
using namespace std;
template<typename Iterator,typename T>
struct accumulate_block{
    void operator()(Iterator first,Iterator last,T& result){//每个子序列累加，不能通过线程的返回值返回累加结果，而是通过一个result引用将结果返回给主线程
        result=std::accumulate(first,last,result);
    }
};
 
template<typename Iterator,typename T>
T parallel_accumulate(Iterator first,Iterator last,T init)
{
    unsigned long const length=std::distance(first,last);//计算序列的长度
    if(!length)//若序列为空直接返回init
        return init;
    unsigned long const min_per_thread=25;//每个线程的子序列大小
    unsigned long const max_threads=(length+min_per_thread-1)/min_per_thread;//计算线程数(向上取整)
    unsigned long const hardware_threads=std::thread::hardware_concurrency();//获取PC的CPU core数目，C++库可能无法访问该信息，所以可能返回0
    unsigned long const num_threads=std::min(hardware_threads!=0?hardware_threads:2,max_threads);//最大线程数在计算出来的最大线程数和硬件core数目中取较小者(线程数超过core数目反而降低性能)
    unsigned long const block_size=length/num_threads;//重新计算每个线程需要执行的序列大小
    vector<T> results(num_threads);//每个线程的结果将保存在results中
    vector<thread> threads(num_threads-1);//开启线程池，只用开启num_threads-1个子线程，因为主线程也可以计算一个序列
    Iterator block_start=first;//序列开始位置
    for(int i=0;i<(num_threads-1);i++){//这里只分配子线程的任务序列
        Iterator block_end=block_start;
        std::advance(block_end,block_size);//迭代器block_end向前移动block_size
        threads[i]=thread(accumulate_block<Iterator,T>(),block_start,block_end,std::ref(results[i]));//每个子线程的子序列分配
        block_start=block_end;//更新序列位置
    }
    accumulate_block<Iterator,T>()(block_start,last,results[num_threads-1]);//主线程的任务，注意是last
    std::for_each(threads.begin(),threads.end(),std::mem_fn(&thread::join));//等待子线程完成
    return std::accumulate(results.begin(),results.end(),init);//汇总结果
}

void test()
{
    std::vector<double> rawData(1000000000);
    std::cout << "Accumulating " << rawData.size() << std::endl;
    for (size_t i = 0; i < rawData.size(); i++)
    {
        rawData[i] = (double) rand() / RAND_MAX;
        //std::cout << rawData[i] << std::endl;
    }
    clock_t start = clock();
    double result = 0.0;
    result = parallel_accumulate(begin(rawData), end(rawData), result);
    long diff = clock() - start;
    std::cout << "Parallel result: " << result << " in "
            << (double) diff / CLOCKS_PER_SEC << " seconds" << std::endl;

    start = clock();
    result = 0.0;
    result = std::accumulate(begin(rawData), end(rawData), result);
    diff = clock() - start;
    std::cout << "Serial result: " << result << " in "
            << (double) diff / CLOCKS_PER_SEC << " seconds" << std::endl;

}
void test2(){
   vector<int> vec;
    for(int i=0;i<100;i++){
        vec.push_back(10);
    }
    int sum=parallel_accumulate(vec.begin(),vec.end(),5);
    cout<<"sum="<<sum<<endl;
}
int main(){
    test();
    return 0; 

}
