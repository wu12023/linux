#include <vector>
#include <queue>
#include <future>
#include <thread>
#include <functional>
#include <atomic>
#include <vector>
#include <algorithm>
#include <fstream>
#include <random>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <optional>
#include <filesystem>
#include <iostream>
template<typename T>
class ThreadSafeQueue {
private:
    std::queue<T> queue;
    std::mutex mutex;
    std::condition_variable cond;

public:
    ThreadSafeQueue() = default;
    ThreadSafeQueue(const ThreadSafeQueue<T> &) = delete; // disable copying
    ThreadSafeQueue& operator=(const ThreadSafeQueue<T> &) = delete; // disable assignment

    std::optional<T> pop() {
        std::unique_lock<std::mutex> mlock(mutex);
        if (queue.empty()) {
            return {};
        }
        auto item = queue.front();
        queue.pop();
        return item;
    }


    void push(const T& item) {
        std::unique_lock<std::mutex> mlock(mutex);
        queue.push(item);
        mlock.unlock();
        cond.notify_one();
    }

    bool empty() {
        std::unique_lock<std::mutex> mlock(mutex);
        return queue.empty();
    }
};
class ThreadPool {
public:
    ThreadPool(size_t threads) : stop(false) {
        for(size_t i = 0;i<threads;++i)
            workers.emplace_back([this] {
                for (;;) {
                    std::optional<std::function<void()>> task_opt = this->tasks.pop();
                    if (!task_opt.has_value()) {
                        if (this->stop) {
                            return;
                        }
                        std::this_thread::yield(); // 避免忙等待
                        continue;
                    }
                    std::function<void()> task = task_opt.value();
                    task();
                }

            });
    }
    void wait() {
        for (std::thread &worker : workers) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }

    // 用于读取文件块的函数
    static std::vector<int64_t> readChunk(std::ifstream& inputFile, size_t chunkSize) {
        std::vector<int64_t> data(chunkSize);
        for (size_t i = 0; i < chunkSize && inputFile; ++i) {
            inputFile >> data[i];
        }
        return data;
    }

    // 用于将数据写入临时文件的函数，并返回临时文件的名称
    static std::string writeTempFile(const std::vector<int64_t>& data) {
        static std::atomic<int> tempFileCounter = 0;
        std::string tempFileName = "tempFile" + std::to_string(tempFileCounter++) + ".txt";
        std::ofstream tempFile(tempFileName);
        for (const auto& item : data) {
            tempFile << item << "\n";
        }
        return tempFileName;
    }

    template<class F, class... Args>
    auto enqueue(F&& f, Args&&... args) 
        -> std::future<typename std::result_of<F(Args...)>::type>
    {
        using return_type = typename std::result_of<F(Args...)>::type;

        auto task = std::make_shared< std::packaged_task<return_type()> >(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...)
        );
        
        std::future<return_type> res = task->get_future();
        if(stop)
            throw std::runtime_error("enqueue on stopped ThreadPool");

        tasks.push([task](){ (*task)(); });
        return res;
    }

    ~ThreadPool() {
        stop = true;
        for(std::thread &worker: workers)
            worker.join();
    }

private:
    std::vector< std::thread > workers;
    ThreadSafeQueue< std::function<void()> > tasks; // 使用无锁队列
    std::atomic<bool> stop;
};



void merge(std::vector<int64_t>& arr, int64_t l, int64_t m, int64_t r) {
    int64_t i, j, k;
    int64_t n1 = m - l + 1;
    int64_t n2 = r - m;

    std::vector<int64_t> L(n1), R(n2);

    for (i = 0; i < n1; i++)
        L[i] = arr[l + i];
    for (j = 0; j < n2; j++)
        R[j] = arr[m + 1 + j];

    i = 0;
    j = 0;
    k = l;
    while (i < n1 && j < n2) {
        if (L[i] <= R[j]) {
            arr[k] = L[i];
            i++;
        }
        else {
            arr[k] = R[j];
            j++;
        }
        k++;
    }

    while (i < n1) {
        arr[k] = L[i];
        i++;
        k++;
    }

    while (j < n2) {
        arr[k] = R[j];
        j++;
        k++;
    }
}


void mergeSort(std::vector<int64_t>& arr, int64_t l, int64_t r) {
    if (l < r) {
        int64_t m = l + (r - l) / 2;

        mergeSort(arr, l, m);
        mergeSort(arr, m + 1, r);

        merge(arr, l, m, r);
    }
}

std::vector<int64_t> readFile(const std::string& filename) {
    std::ifstream file(filename, std::ios::binary | std::ios::in);
    std::vector<int64_t> data;

    int64_t value;
    while (file.read(reinterpret_cast<char*>(&value), sizeof(value))) {
        data.push_back(value);
    }

    return data;
}

void writeFile(const std::string& filename, const std::vector<int64_t>& data) {
    std::ofstream file(filename, std::ios::binary | std::ios::out);

    for (const auto& value : data) {
        file.write(reinterpret_cast<const char*>(&value), sizeof(value));
    }
}

void processData(const std::string& inputFilename, const std::string& outputFilename) {
    ThreadPool pool(2);  // Create a pool with 2 threads

    auto data = readFile(inputFilename);  // Read the input file

    // Sort the data using the thread pool
    pool.enqueue([&data]() { mergeSort(data, 0, data.size() - 1); }).get();

    writeFile(outputFilename, data);  // Write the sorted data to the output file
}

void generateRandomFile(const std::string &filename, size_t numValues) {
    std::ofstream file(filename, std::ios::binary);
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int64_t> dist(INT64_MIN, INT64_MAX);

    for (size_t i = 0; i < numValues; ++i) {
        int64_t value = dist(gen);
        file.write(reinterpret_cast<char*>(&value), sizeof(int64_t));
    }
}

void mergeFiles(const std::vector<std::string> &inputFilenames, const std::string &outputFilename) {
    // 打开输入文件
    std::vector<std::ifstream> inputFiles;
    for (const auto &filename : inputFilenames) {
        inputFiles.emplace_back(filename, std::ios::binary);
    }

    // 合并输入文件
    std::ofstream outputFile(outputFilename, std::ios::binary);
    std::priority_queue<std::pair<int64_t, int>, std::vector<std::pair<int64_t, int>>, std::greater<>> pq;
    for (int i = 0; i < inputFiles.size(); ++i) {
        int64_t value;
        if (inputFiles[i].read(reinterpret_cast<char*>(&value), sizeof(int64_t))) {
            pq.push({value, i});
        }
    }
    while (!pq.empty()) {
        auto [value, i] = pq.top();
        pq.pop();
        outputFile.write(reinterpret_cast<char*>(&value), sizeof(int64_t));
        if (inputFiles[i].read(reinterpret_cast<char*>(&value), sizeof(int64_t))) {
            pq.push({value, i});
        }
    }
}


void sortFile(const std::string &inputFilename, const std::string &outputFilename, size_t memoryLimit) {
    // 计算每个块可以包含的最大元素数量
    size_t maxValuesPerBlock = memoryLimit / sizeof(int64_t);

    // 读取输入文件
    std::ifstream inputFile(inputFilename, std::ios::binary);
    std::vector<std::string> tempFilenames;
    std::vector<int64_t> values;
    values.reserve(maxValuesPerBlock);
    int64_t value;
    while (inputFile.read(reinterpret_cast<char*>(&value), sizeof(int64_t))) {
        values.push_back(value);
        if (values.size() == maxValuesPerBlock) {
            // 对块进行排序并写回到磁盘
            std::sort(values.begin(), values.end());
            std::string tempFilename = inputFilename + ".tmp" + std::to_string(tempFilenames.size());
            tempFilenames.push_back(tempFilename);
            std::ofstream outputFile(tempFilename, std::ios::binary);
            for (auto value : values) {
                outputFile.write(reinterpret_cast<char*>(&value), sizeof(int64_t));
            }
            values.clear();
        }
    }
    // 处理剩余的元素
    if (!values.empty()) {
        std::sort(values.begin(), values.end());
        std::string tempFilename = inputFilename + ".tmp" + std::to_string(tempFilenames.size());
        tempFilenames.push_back(tempFilename);
        std::ofstream outputFile(tempFilename, std::ios::binary);
        for (auto value : values) {
            outputFile.write(reinterpret_cast<char*>(&value), sizeof(int64_t));
        }
    }

    // 合并所有排序后的块
    mergeFiles(tempFilenames, outputFilename);

    // 删除临时文件
    for (auto &filename : tempFilenames) {
        std::remove(filename.c_str());
    }
}


bool sortFileWrapper(const std::string& path, const std::string& output, size_t bufferSize) {
    sortFile(path, output, bufferSize);
    // 检查文件是否已排序或其他检查
    // 返回 true 如果排序成功，否则返回 false
    return true;
}
int main() {
    ThreadPool pool(2);
    std::vector<std::future<bool>> futures;
    std::vector<std::string> tempFiles;
    std::cout << "Program started\n";
    std::cout.flush();
    for (const auto &entry : std::filesystem::directory_iterator("C:\\Users\\wu\\Desktop\\linux\\期末作业")) {
        std::cout << "Processing file: " << entry.path() << "\n";
        futures.push_back(pool.enqueue([&pool, &entry, &tempFiles] {
            // 定义块的大小。这应该根据可用内存进行调整。
            const size_t CHUNK_SIZE = 1024 * 1024; // 1MB

            std::ifstream inputFile(entry.path().string());

            for (;;) {
                std::vector<int64_t> data = pool.readChunk(inputFile, CHUNK_SIZE);
                if (data.empty()) {
                    break;
                }
                pool.enqueue([&pool, data, &tempFiles]() mutable {
                    std::sort(data.begin(), data.end());
                    std::string tempFile = pool.writeTempFile(data);
                    tempFiles.push_back(tempFile);
                });
            }

            // 等待所有任务完成
            pool.wait();

            // 归并排序临时文件
            // 先定义一个结构体，用于保存每个临时文件的当前最小值和对应的输入流
            struct MergeHelper {
                int64_t value;
                std::ifstream* stream;
                bool operator<(const MergeHelper& other) const { return value > other.value; }
            };
            
            std::priority_queue<MergeHelper> pq;
            for (const auto& tempFileName : tempFiles) {
                std::ifstream* stream = new std::ifstream(tempFileName);
                int64_t value;
                if (*stream >> value) {
                    pq.push(MergeHelper{value, stream});
                }
            }

            std::ofstream outFile("sorted_" + entry.path().filename().string());
            while (!pq.empty()) {
                MergeHelper mh = pq.top();
                pq.pop();
                outFile << mh.value << "\n";
                if (*mh.stream >> mh.value) {
                    pq.push(mh);
                } else {
                    delete mh.stream;
                }
            }

            for (const auto& tempFileName : tempFiles) {
                std::remove(tempFileName.c_str());
            }

            return true;
        }));
    }

    for (auto& fut : futures) {
        try {
            if (fut.get()) {
                std::cout << "Sort successful\n";
            } else {
                std::cout << "Sort failed\n";
            }
        } catch (const std::exception& e) {
            std::cout << "Exception caught: " << e.what() << "\n";
        }
    }


    return 0;
}
