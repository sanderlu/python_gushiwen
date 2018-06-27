from base_setting import *
from resource import user_agents, ips


class Authors:
    mem_key = 'author_url_'
    url = 'https://so.gushiwen.org/authors/Default.aspx?p={}&c={}'
    id = 1

    def __authors_save(self, authors):
        return mongo_obj.mg('adds', 'author', authors)

    def __get_chaodai_authors(self, dynasty, cid):
        # 随机获取一个User-agent
        user_agent = user_agents[random.randint(0, len(user_agents)-1)]

        # 随机获取一个代理ip
        # ip = self.ips[random.randint(0, len(self.ips))]

        current_page = 1
        authors = []
        while True:
            # 请求url
            real_url = self.url.format(current_page, dynasty)
            try:
                # request = requests.get(real_url, timeout=3, headers={"User-Agent": user_agent}, proxies={"https": "https://"+ip})
                request = requests.get(real_url, timeout=10, headers={"User-Agent": user_agent})
            except requests.RequestException as e:
                # 如果发生异常，记录并跳过异常url，等待三秒继续执行
                print(e)
                print("error url is "+real_url)
                current_page += 1
                time.sleep(3)
                continue

            soup = BeautifulSoup(request.text, 'lxml')

            # 列表中每个诗人信息的div
            sonspics = soup.select(' div.sonspic')
            if not sonspics:
                break
            current_page += 1

            for i in range(0, len(sonspics)):
                name = sonspics[i].select(' div.cont > p:nth-of-type(1) > a:nth-of-type(1) > b')
                img = sonspics[i].select(' div.cont > div > a > img')
                info = sonspics[i].select(' div.cont > p:nth-of-type(2)')
                url = sonspics[i].select(' div.cont > p:nth-of-type(2) > a')
                praise = sonspics[i].select(' div.tool > div.good > a > span ')

                url_unique = url[0].get('href') if url else ''
                if url_unique:
                    if mem_obj.mc("get", self.mem_key+url_unique):
                        continue
                    else:
                        mem_obj.mc('set', self.mem_key + url_unique, self.id)

                authors.append(
                    {
                        'id': self.id,
                        'name': name[0].text.strip() if name else '',
                        'cid': cid,
                        'img': img[0].attrs['src'] if img else '',
                        'info': list(info[0].children)[0] if info else '',
                        'url': url[0].get('href') if url else '',
                        'count': int(url[0].text[1:-3]) if url else 0,
                        'praise': int(praise[0].text.strip()) if praise else '0',
                    }
                )
                self.id += 1

            print(dynasty+' current_page:'+str(current_page)+' authors:'+str(len(authors)))
            # time.sleep(0.1)

        return authors

    def go(self):
        print(' begin get authors ... ')
        for i in range(0, len(dynasties)):
            authors = self.__get_chaodai_authors(dynasties[i], i)
            if authors:
                add_res = self.__authors_save(authors)
                print('add mongo result', add_res.acknowledged)
                # 释放本次爬取结果，以免内存占用过大
                del authors
                gc.collect()

authors_obj = Authors()
