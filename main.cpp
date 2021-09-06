#include <iostream>
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <thread>
#include <cstring>
#include "blocking_queue.hpp"

/* Пример входных данных:
 * num1(4 байта)num2(4 байта)num3(4 байта)...
 * */

/* Задача, которую выполняет сортирующий поток:
 * 1) отступ в файле
 * 2) максимальное количество чисел (int) для сортировки
 * 3) файл, из которого производится чтение
 * 4) MPMC-очередь, в которую записываем имя отсортированного файла
 * */
struct SortTask {
    size_t offset;
    size_t max_numbers;
    std::string input_file;
    tp::UnboundedBlockingQueue<std::string>& sorted_parts;
};

void SorterRoutine(SortTask task) {
    std::string output_filename = std::to_string(task.offset);

    std::ifstream istr(task.input_file, std::ios_base::binary);
    std::ofstream ostr(output_filename); // <- отсортированные числа
    istr.seekg(task.offset);

    std::vector<int> numbers_for_sort;
    numbers_for_sort.reserve(task.max_numbers);
    for (size_t i = 0; i < task.max_numbers && !istr.eof(); i++) {
        int number = 0;
        istr.read((char*)&number, sizeof(number));
        numbers_for_sort.push_back(number);
    }

    std::sort(numbers_for_sort.begin(), numbers_for_sort.end());

    if (!numbers_for_sort.empty()) {
        std::copy(numbers_for_sort.begin(), numbers_for_sort.end(),
                  std::ostream_iterator<int>(ostr, " "));
        task.sorted_parts.Put(output_filename);
    }
}

/* param: filename - файл для сортировки
 * param: sorted_parts - MPMC-очередь с отсортированными кусками файла
 * param: batch_size - максимальное количество чисел для сортировки
 * ret: Количество файлов, готовых к слиянию
 * */
size_t SortBatchFiles(const std::string& filename,
                      tp::UnboundedBlockingQueue<std::string>& sorted_parts,
                      size_t batch_size)
{
    size_t file_size = std::filesystem::file_size(filename);
    size_t batch_bytes = batch_size * sizeof(int);
    size_t merge_file_count = 0;

    for (size_t i = 0; i < file_size / batch_bytes + 1; i++) {
        SortTask thread_task{i * batch_bytes, batch_size, filename, sorted_parts};
        if (thread_task.offset >= file_size) {
            break;
        }

        ++merge_file_count;
        /* Запускаем задачу, которую исполняет поток */
        std::thread([thread_task]() {
            SorterRoutine(thread_task);
        }).detach();
    }

    return merge_file_count;
}

/* Слияние отсортированных файлов */

/* Задача, которую выполняет "сливающий" поток
 * 1) filename1 - имя первого файла для слияния
 * 2) filename2 - имя второго файла для слияния
 * 3) sorted_parts - MPMC-очередь, в которую записываем имя отсортированного файла
 * 4) id - уникальный идендификатор потока (для создания имени файла)
 * */
struct MergeTask {
    std::string filename1;
    std::string filename2;
    tp::UnboundedBlockingQueue<std::string>& sorted_parts;
    int id;
};

void MergeRoutine(MergeTask task) {
    std::string output_filename = std::to_string(task.id);

    std::ifstream istr1(task.filename1);
    std::ifstream istr2(task.filename2);
    std::ofstream ostr(output_filename);
    int value1 = 0;
    int value2 = 0;

    istr1 >> value1;
    istr2 >> value2;
    while (!istr1.eof() && !istr2.eof()) {
        if (value1 < value2) {
            ostr << value1 << " ";
            istr1 >> value1;
        } else {
            ostr << value2 << " ";
            istr2 >> value2;
        }
    }

    while (!istr1.eof()) {
        ostr << value1 << " ";
        istr1 >> value1;
    }

    while (!istr2.eof()) {
        ostr << value2 << " ";
        istr2 >> value2;
    }

    task.sorted_parts.Put(output_filename);
}

/* Объединяет отсортированные части
 * param: sorted_parts - собственно имена файлов
 * param: tasks_count - количество файлов для слияния
 * ret: Имя отсортированного файла
 * */
std::string MergeBatchFiles(tp::UnboundedBlockingQueue<std::string>& sorted_parts,
                            size_t tasks_count)
{
    int id = -1;
    while (tasks_count-- > 1) {
        MergeTask thread_task{*sorted_parts.Take(), *sorted_parts.Take(), sorted_parts, --id};

        /* Запускаем 'сливающую' подзадачу */
        std::thread([thread_task]() {
            MergeRoutine(thread_task);
        }).detach();
    }

    /* Сейчас в очереди находится один отсортированный файл, он нам и нужен */
    return *sorted_parts.Take();
}

/* Проверяет, отсортирован ли файл */
bool IsSorted(const std::string& filename) {
    std::vector<int> vec;
    std::ifstream istr(filename);

    while (!istr.eof()) {
        int value = 0;
        istr >> value;

        vec.push_back(value);
    }
    /* В силу формата записи последнее прочитанное значение - 0 */
    if (!vec.empty()) {
        vec.pop_back();
    }

    return std::is_sorted(vec.begin(), vec.end());
}

// argv[1] = filename
// argv[2] = max_numbers
int main(int argc, char* argv[]) {
    std::string filename = std::string(argv[1], strlen(argv[1]));
    tp::UnboundedBlockingQueue<std::string> sorted_parts;
    size_t batch_size = std::atoi(argv[2]);
    std::string output_file;

    size_t merge_files_count = SortBatchFiles(filename, sorted_parts, batch_size);
    output_file = MergeBatchFiles(sorted_parts, merge_files_count);
    std::cout << output_file << std::endl;

    return 0;
}