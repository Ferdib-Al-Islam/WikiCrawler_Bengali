from MyCrawler import BanglaCrawler, IterableQueue, CrawlerUtil
import queue
from concurrent.futures import ThreadPoolExecutor as Executor

ROOT_URL = 'https://bn.wikipedia.org/wiki/%E0%A6%A8%E0%A6%A6%E0%A7%80'

QUEUE = queue.Queue()
QUEUE.put(ROOT_URL)
utill = CrawlerUtil()

# hold all the urls that had been processed. This helps not use
PROCESSED_URL_SET = utill.get_previous_processed_url()


def fetch(url):
    print('Fetching ' + url)
    bc = BanglaCrawler()
    return bc.crawle(url)


maximumNumber = 100
cntr = 0

with Executor(max_workers=50) as exe:
    while cntr < maximumNumber:
        url_list = []
        for url in IterableQueue(QUEUE):
            url_list.append(url)
            PROCESSED_URL_SET.add(url)  # actually process starts from exe.submit() in below

        # clear the queue
        with QUEUE.mutex:
            QUEUE.queue.clear()

        jobs = [exe.submit(fetch, u) for u in url_list]
        links = [job.result() for job in jobs]

        all_url_flat_list = [item for sublist in links for item in sublist]

        for url in all_url_flat_list:
            if url not in PROCESSED_URL_SET:
                QUEUE.put(url)
                cntr += 1
                print('')
            else:
                print('\nDuplicate URL. Skipping ' + url + '\n')

        utill.save_current_state(PROCESSED_URL_SET)
