from base_setting import *
from resource import user_agents, ips


class Sons:
    mem_key = 'son_num_'
    id = 1
    author_id = 1
    file_io = False

    def __log_add(self, log):
        if not self.file_io:
            self.file_io = open('./sons.log', 'a')
        self.file_io.write('\n'+time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + ' ' + str(log))

    def __io_close(self):
        self.file_io.close()

    def __get_author_by_id(self, id):
        return mongo_obj.mg('find_one', 'author', {"id": id})

    def __get_son(self, author):

        # 随机获取一个User-agent
        user_agent = user_agents[random.randint(0, len(user_agents) - 1)]

        # 随机获取一个代理ip
        # ip = self.ips[random.randint(0, len(self.ips))]

        #   https://so.gushiwen.org/authors/authorvsw_b90660e3e492A{}.aspx
        url = so_url + author['url'][1:-6] + '{}.aspx'

        current_page = 1
        sons = []

        while True:
            # 请求url
            real_url = url.format(current_page)
            # real_url = "https://so.gushiwen.org/authors/authorvsw_3b99a16ff2ddA1.aspx"

            try:
                # request = requests.get(real_url, timeout=3, headers={"User-Agent": user_agent}, proxies={"https": "https://"+ip})
                request = requests.get(real_url, timeout=10, headers={"User-Agent": user_agent})
            except requests.RequestException as e:
                # 如果发生异常，记录并跳过异常url，等待三秒继续执行
                self.__log_add(e)
                self.__log_add("error url is " + real_url)
                current_page += 1
                time.sleep(3)
                continue

            soup = BeautifulSoup(request.text, 'lxml')

            # 列表中每个诗人信息的div
            son_list = soup.select(' div.sons')
            if not son_list:
                break
            if current_page > ((author['count'] // 10) + 1):
                self.__log_add('max_page_err ' + author['name'] + ' count ' + str(current_page))
                break
            current_page += 1
            for i in range(0, len(son_list)):
                name = son_list[i].select(' div.cont > p:nth-of-type(1) > a')
                dynasty = son_list[i].select(' div.cont > p:nth-of-type(2) > a:nth-of-type(1)')
                content = son_list[i].select(' div.cont > div.contson')
                praise = son_list[i].select(' div.tool > div.good > a > span')
                tags = son_list[i].select(' div.tag > a')

                num_unique = name[0].get('href')[9:-5] if name else ''
                if num_unique:
                    if mem_obj.mc("get", self.mem_key + num_unique):
                        continue
                    else:
                        mem_obj.mc('set', self.mem_key + num_unique, self.id)
                try:
                    info = {
                        'id': self.id,
                        'aid': self.author_id,
                        'num': num_unique,
                        'name': name[0].select(' b ')[0].text if name else '',
                        'cid': 0,
                        'praise': praise[0].text.strip() if praise else '0',
                        'content': [],
                        'tags': []
                        }

                    if dynasty:
                        if dynasty[0].text and (dynasty[0].text not in dynasties):
                            dynasties.append(dynasty[0].text)
                            self.__log_add('addDynasty: ' + dynasty[0].text)
                        info['cid'] = dynasties.index(dynasty[0].text)
                    # 检验诗的分类是否在预定义分类中，如果在则将分类id赋予tags
                    if tags:
                        for tag in tags:
                            if tag.text.strip() not in sons_types:
                                sons_types.append(tag.text.strip())
                                self.__log_add('addType: '+tag.text.strip())
                            info['tags'].append(sons_types.index(tag.text.strip()))

                    son_p = content[0].select(' p')
                    if son_p:
                        for j in son_p:
                            info['content'].append(j.text.strip())
                    else:
                        content_list = content[0].contents
                        for x in range(0, len(content_list)):
                            if content_list[x].name != 'br':
                                info['content'].append(str(content_list[x]).strip('\n'))

                    sons.append(info)
                    self.id += 1
                    del info

                except Exception:
                    self.__log_add('appendError' + author['name'] + str(current_page))
                    continue
            # self.__log_add(author['name'] + ' current_page:' + str(current_page - 1))
            # time.sleep(0.1)
        return sons

    def __son_save(self, info):
        return mongo_obj.mg('adds', 'son', info)

    def go(self):
        print(' begin get sons ... ')
        # 跟据诗人id自增遍历
        while True:
            author = self.__get_author_by_id(self.author_id)
            if author:
                son_list = self.__get_son(author)
                if son_list:
                    add_res = self.__son_save(son_list)
                    if not add_res.acknowledged:
                        self.__log_add('add mongo error ' + str(son_list))
                    else:
                        self.__log_add('get success by ' + author['name'])
                        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + ' get success by ' + author['name'])
                    # 释放本次爬取结果，以免内存占用过大
                    del son_list
                    gc.collect()
                else:
                    pass
                    # self.__log_add('未获取到 {} 的诗歌'.format(author['name']))
            else:
                self.__log_add(sons_types)
                self.__log_add(dynasties)
                self.__io_close()
                break
            self.author_id += 1

        # self.__get_son()

sons_obj = Sons()