#ifndef SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_
#define SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_

#include <atomic>
#include <mutex>
#include <thread>

#include <algorithm>
#include <cmath>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "immutable/network.hpp"
#include "immutable/pageIdAndRank.hpp"
#include "immutable/pageRankComputer.hpp"

class MultiThreadedPageRankComputer : public PageRankComputer {
private:
    uint32_t numThreads;

    /**
     * Bariera o ustawialnej odpornosci (ilosc watkow)
     * Po wywolaniu procedury reach() przez dany watek, czeka on na reszte watkow by przejsc do
     * kolejnego etapu.
     */
    class Bariera {
    private:
        std::mutex mutex;
        std::mutex waits_on;
        int reached;
        int resistance;

    public:
        Bariera(int res)
        {
            waits_on.lock();
            reached = 0;
            resistance = res;
        };

        void reach()
        {
            mutex.lock();
            reached++;
            if (reached != resistance) {
                mutex.unlock();
                waits_on.lock();
            }
            reached--;
            if (reached == 0)
                mutex.unlock();
            else
                waits_on.unlock();
        }
    };

    /**
     * Funkcja wątku naszego komputera
     * @param id                        ID watku
     * @param network                   Network na ktorym operuje caly komputer
     * @param alpha                     Parametr alpha
     * @param iterations                Ilosc mozliwych iteracji
     * @param tolerance                 Wymagana tolerancja wyniku
     * @param pageHashMap               Udostepniona przez referencje dla kazdego watku mapa
     * @param pageHashIteratorVec       Udostepniony przez referencje dla kazdego watku vector zawierajacy
     *                                  informacje dla kazdego watku, ktora czesc mapy pageHashMap ma przerobic
     * @param numLinks                  Udostepniona przez referencje dla kazdego watku mapa liczby polaczen
     * @param danglingNodes             Udostepniony przez referencje dla kazdego watku set node'ow
     * @param danglingNodesIteratorVec  Udostepniony przez referencje dla kazdego watku vector zawierajacy
     *                                  informacje dla kazdego watku, ktora czesc setu danglingNodesIteratorVec
     *                                  ma przerobic
     * @param edges                     Udostepniona przez referencje dla kazdego watku mapa grafi
     * @param dangleSum                 Udostepniona przez referencje dla kazdego watku zebrana suma
     * @param difference                Udostepniona przez referencje dla kazdego watku zebrana różnica
     * @param result                    Rezultat funkcji
     * @param b                         Bariera na której będą czekać wątki
     * @param aint                      Atomic int wymagany to przechodzenia po początkowej tablicy network.getPages()
     * @param previousPageHashMap       Udostepniona przez referencje dla kazdego watku mapa przechowujaca stan
     * @param end                       Udostepniona przez referencje dla kazdego watku boolean zaznaczajacy
     *                                  czy watek powinien sie skonczyc czy tez i nie
     * @param numThreads                Wartosc reprezentujaca ile watkow zostalo uruchomionych
     * @param mutex                     
     */
    static void watek(uint32_t id,
        Network const& network,
        double alpha,
        uint32_t iterations,
        double tolerance,
        std::unordered_map<PageId, PageRank, PageIdHash>& pageHashMap,
        std::vector<std::unordered_map<PageId, PageRank, PageIdHash>::iterator>& pageHashIteratorVec,
        std::unordered_map<PageId, uint32_t, PageIdHash>& numLinks,
        std::unordered_set<PageId, PageIdHash>& danglingNodes,
        std::vector<std::unordered_set<PageId, PageIdHash>::iterator>& danglingNodesIteratorVec,
        std::unordered_map<PageId, std::vector<PageId>, PageIdHash>& edges,
        double& dangleSum,
        double& difference,
        std::vector<PageIdAndRank>& result,
        Bariera& b,
        std::atomic<int>& aint,
        std::unordered_map<PageId, PageRank, PageIdHash>& previousPageHashMap,
        bool& end,
        uint32_t numThreads,
        std::mutex& mutex)
    {
        b.reach();
        int network_size = network.getSize();

        // Zrownoleglenie generowania ID.
        int index = aint++;
        while (index < network_size) {
            auto const& page = network.getPages()[index];
            page.generateId(network.getGenerator());
            index = aint++;
        }

        b.reach();

        // Ustalam, ze liderem watkow jest watek o id rownym 0 i to on ustawia wszystkie
        // wartosci uzywane dalej.
        if (id != 0) {
            b.reach();
        } else {
            for (auto const& page : network.getPages()) {
                auto page_id = page.getId();
                pageHashMap[page_id] = 1.0 / network_size;
                numLinks[page_id] = page.getLinks().size();
                if (numLinks[page_id] == 0) {
                    danglingNodes.insert(page_id);
                }
                for (auto link : page.getLinks()) {
                    edges[link].push_back(page_id);
                }
            }

            // Ustawia, które wątki będą dalej przetwarzać ktore miejsca danglingNodes
            int currentIndex = 0;
            int size = danglingNodes.size();
            int perThread = std::max(1, (int)(size / numThreads));
            uint32_t first_get_more = size - perThread * numThreads;
            auto iterator = danglingNodes.begin();
            danglingNodesIteratorVec.push_back(iterator);
            for (uint32_t i = 0; i < numThreads - 1; i++) {
                if (currentIndex + perThread + (i < first_get_more) < size) {
                    std::advance(iterator, perThread + (i < first_get_more));
                    currentIndex += perThread + (i < first_get_more);
                } else {
                    iterator = danglingNodes.end();
                    currentIndex = size + 1;
                }
                danglingNodesIteratorVec.push_back(iterator);
            }
            danglingNodesIteratorVec.push_back(danglingNodes.end());

            // Ustawia, które wątki będą dalej przetwarzać ktore miejsca pageHashMap
            currentIndex = 0;
            size = pageHashMap.size();
            int perThread2 = std::max(1, (int)(size / numThreads));
            first_get_more = size - perThread2 * numThreads;
            auto iterator2 = pageHashMap.begin();
            pageHashIteratorVec.push_back(iterator2);
            for (uint32_t i = 0; i < numThreads - 1; i++) {
                if (currentIndex + perThread2 + (i < first_get_more) < size) {
                    std::advance(iterator2, perThread2 + (i < first_get_more));
                    currentIndex += perThread2 + (i < first_get_more);
                } else {
                    iterator2 = pageHashMap.end();
                    currentIndex = size + 1;
                }
                pageHashIteratorVec.push_back(iterator2);
            }
            pageHashIteratorVec.push_back(pageHashMap.end());

            dangleSum = 0;
            previousPageHashMap = pageHashMap;
            b.reach();
        }

        for (uint32_t i = 0; i < iterations; ++i) {
            // Zrownoleglenie liczenia sum.
            double mySum = 0;
            auto mybegin = danglingNodesIteratorVec[id];
            auto myend = danglingNodesIteratorVec[id + 1];
            while (mybegin != myend) {
                mySum += previousPageHashMap[*mybegin];
                mybegin++;
            }
            mutex.lock();
            dangleSum += mySum;
            mutex.unlock();
            b.reach();

            if (id != 0) {
                b.reach();
            } else {
                dangleSum *= alpha;
                difference = 0;
                b.reach();
            }

            // Zrownoleglenie liczenie difference.
            double myDif = 0;
            auto mybegin2 = pageHashIteratorVec[id];
            auto myend2 = pageHashIteratorVec[id + 1];
            while (mybegin2 != myend2) {
                PageId pageId = (*mybegin2).first;
                double danglingWeight = 1.0 / network_size;
                (*mybegin2).second = dangleSum * danglingWeight + (1.0 - alpha) / network_size;

                if (edges.count(pageId) > 0) {
                    for (auto link : edges[pageId]) {
                        (*mybegin2).second += alpha * previousPageHashMap[link] / numLinks[link];
                    }
                }
                myDif += std::abs(previousPageHashMap[pageId] - pageHashMap[pageId]);
                mybegin2++;
            }
            mutex.lock();
            difference += myDif;
            mutex.unlock();
            b.reach();

            // Lider sprawdza czy difference spełnia warunek.
            if (id != 0) {
                b.reach();
            } else {
                if (difference < tolerance) {
                    for (auto iter : pageHashMap) {
                        result.push_back(PageIdAndRank(iter.first, iter.second));
                    }
                    ASSERT(result.size() == network.getSize(),
                        "Invalid result size=" << result.size() << ", for network" << network);

                    end = true;
                }
                previousPageHashMap = pageHashMap;
                dangleSum = 0;
                b.reach();
            }
            if (end)
                break;
            if (i == iterations - 1)
                ASSERT(false, "Not able to find result in iterations=" << iterations);
        }
    }

public:
    MultiThreadedPageRankComputer(uint32_t numThreadsArg)
        : numThreads(numThreadsArg){};

    std::vector<PageIdAndRank>
    computeForNetwork(Network const& network, double alpha, uint32_t iterations,
        double tolerance) const
    {
        std::unordered_map<PageId, PageRank, PageIdHash> pageHashMap;
        std::vector<std::unordered_map<PageId, PageRank, PageIdHash>::iterator> pageHashIteratorVec;
        std::unordered_map<PageId, uint32_t, PageIdHash> numLinks;
        std::unordered_set<PageId, PageIdHash> danglingNodes;
        std::vector<std::unordered_set<PageId, PageIdHash>::iterator> danglingNodesIteratorVec;
        std::unordered_map<PageId, std::vector<PageId>, PageIdHash> edges;
        std::vector<std::unordered_map<PageId, PageRank, PageIdHash>::iterator> vec;
        double dangleSum;
        double difference;
        std::vector<PageIdAndRank> result;
        Bariera b(numThreads);
        std::atomic<int> aint(0);
        std::unordered_map<PageId, PageRank, PageIdHash> previousPageHashMap;
        bool end = false;
        std::mutex mutex;

        std::vector<std::thread> threads;
        for (size_t i = 0; i < numThreads; i++) {
            threads.push_back(std::thread(watek, i, std::ref(network), alpha, iterations, tolerance,
                std::ref(pageHashMap), std::ref(pageHashIteratorVec),
                std::ref(numLinks),
                std::ref(danglingNodes),
                std::ref(danglingNodesIteratorVec),
                std::ref(edges),
                std::ref(dangleSum), std::ref(difference),
                std::ref(result), std::ref(b), std::ref(aint),
                std::ref(previousPageHashMap), std::ref(end),
                numThreads, std::ref(mutex)));
        }

        for (std::thread& t : threads) {
            t.join();
        }

        return result;
    }

    std::string getName() const
    {
        return "MultiThreadedPageRankComputer[" + std::to_string(this->numThreads) + "]";
    }
};

#endif /* SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_ */
