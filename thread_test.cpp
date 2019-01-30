/*
 *多线程版本mapreduce
 * */
#include <iostream>
#include <list>
#include <numeric>
#include <vector>
#include <thread>
#include <future>

using namespace std;
template<typename Iterator,typename T>
struct accumulate_block
{
    T operator()(Iterator first, Iterator last)
    {
        std::thread::id id = std::this_thread::get_id();
        return std::accumulate(first, last, T());
    }
};

class join_threads
{
    std::vector<std::thread>& threads;
public:
    explicit join_threads(std::vector<std::thread>& threads_):
    threads(threads_)
    {
        std::thread::id id = std::this_thread::get_id();
    }
    ~join_threads()
    {
        std::thread::id id = std::this_thread::get_id();
        for(unsigned long i=0;i<threads.size();++i)
        {
            if(threads[i].joinable())
                threads[i].join();
        }
    }
};

template<typename Iterator,typename T>
T parallel_accumulate(Iterator first,Iterator last,T init)
{
    std::thread::id id = std::this_thread::get_id();
    unsigned long const length=std::distance(first,last);
    if(!length)
        return init;
    unsigned long const min_per_thread=25;
    unsigned long const max_threads=(length+min_per_thread-1)/min_per_thread;
    unsigned long const hardware_threads=std::thread::hardware_concurrency();
    unsigned long const num_threads=std::min(hardware_threads!=0?hardware_threads:2,max_threads);
    unsigned long const block_size=length/num_threads;
    cout<< "num_threads:"<<num_threads<<endl;
    std::vector<std::future<T> > futures(num_threads-1);
    std::vector<std::thread> threads(num_threads-1);
    join_threads joiner(threads);
    Iterator block_start=first;
    for(unsigned long i=0;i<(num_threads-1);++i)
    {
        Iterator block_end=block_start;
        std::advance(block_end,block_size);
        std::packaged_task<T(Iterator,Iterator)> task(accumulate_block<Iterator,T>{});
        //std::packaged_task<T(Iterator,Iterator)> task(accumulate_block<Iterator,T>());
        futures[i]=task.get_future();
        threads[i]=std::thread(std::move(task),block_start,block_end);
        block_start=block_end;
    }
    T last_result=accumulate_block<Iterator, T>()(block_start,last);
    T result=init;
    for(unsigned long i=0;i<(num_threads-1);++i)
    {
        result+=futures[i].get();
    }
    result += last_result;
    return result;
};


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

int main()
{
test();
/*
    list<int> l;
    for(int i=0; i<1000000; ++i)
        l.push_back(i);

    std::thread::id id = std::this_thread::get_id();
    int res = ::parallel_accumulate(l.begin(), l.end(), 0);
    cout<<res<<endl;
*/
    return 0;

}
