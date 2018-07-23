import os
import datetime
import re
import lxml.html
import MySQLdb
import MySQLdb.cursors
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.keys import Keys

class Crawl:

    MYSQL_HOST = ''
    MYSQL_DBNAME = ''
    MYSQL_USER = ''
    MYSQL_PASSWORD = ''
    browser = None

    finish = 0  # 爬虫完成状态（0：未完成；1：完成）
    crawl_num = 0
    insert_list = []
    title_date = {}
    title_author = {}

    def initialization(self):
        """
        获取数据库信息，配置browser路径
        :return:
        """
        # 获取数据库信息
        Crawl.MYSQL_HOST = '127.0.0.1'
        Crawl.MYSQL_DBNAME = 'zhuanli_spider'
        Crawl.MYSQL_USER = 'root'
        Crawl.MYSQL_PASSWORD = '894757679'
        # 获取当前模块路径的父路径
        father_path = os.path.dirname(os.path.abspath(__file__))
        browser_path = os.path.join(father_path, 'chromedriver.exe')

        # 配置浏览器

        # 不加载图片
        chrome_options = webdriver.ChromeOptions()
        prefs = {"profile.managed_default_content_settings.images": 2}
        chrome_options.add_experimental_option("prefs", prefs)

        # 无界面
        # chrome_options = webdriver.ChromeOptions()
        # chrome_options.add_argument('--headless')
        # chrome_options = chrome_options,

        Crawl.browser = webdriver.Chrome(chrome_options=chrome_options, executable_path=browser_path)
        Crawl.browser.set_page_load_timeout(60)
        Crawl.browser.set_script_timeout(60)

    def start(self):
        """
        当爬虫处于未完成状态，就不停从数据库中获取
        :return:
        """
        while Crawl.finish == 0:
            self.initialization()  # 初始化
            date_type, crawl_state = self.get_crawl_state()
            if date_type != '':
                # 若date_type不为空，开始爬
                if crawl_state == '0':
                    # 从头开始
                    self.new_crawl(date_type, crawl_state)
                else:
                    # 先跳转到已经爬日期的那一页，再开始爬取
                    self.continue_crawl(date_type, crawl_state)
            else:
                # 若date_type为空，说明爬完了,将爬虫状态设置为1，循环结束
                Crawl.finish = 1
            Crawl.browser.quit()

        return 1

    def get_crawl_state(self):
        """
        从数据库中获取要爬的date_type以及对应的爬取状态。
        未爬、爬到哪页、已爬
        :return: date_type, crawl_state
        """

        db = MySQLdb.connect(
            Crawl.MYSQL_HOST,
            Crawl.MYSQL_USER,
            Crawl.MYSQL_PASSWORD,
            Crawl.MYSQL_DBNAME,
            charset='utf8',
            use_unicode=True
        )
        cursor = db.cursor()
        cursor.execute('select date_type, crawl_state from is_crawl where crawl_state != \'已爬\' LIMIT 1')
        data = cursor.fetchall()
        db.close()

        if data:
            date_type = data[0][0]
            crawl_state = data[0][1]
        else:
            date_type = ''
            crawl_state = ''
        print(date_type + '; ' + crawl_state)
        if crawl_state == '未爬':
            crawl_state = '0'
        return date_type, crawl_state

    def new_crawl(self, date_type, crawl_state):
        print('new_crawl')
        crawl_state = int(crawl_state) + 1
        # 进入搜索页面
        browser = self.to_index(date_type, crawl_state)
        # 解析
        self.parse(browser, date_type, crawl_state)

    def continue_crawl(self, date_type, crawl_state):
        print('continue_crawl')
        crawl_state = int(crawl_state) + 1
        # 进入搜索页面
        browser = self.to_index(date_type, crawl_state)
        # 解析
        self.parse(browser, date_type, crawl_state)

    def to_index(self, date_type, crawl_state):

        browser = Crawl.browser

        # 请求网站
        try:
            browser.get('http://epub.sipo.gov.cn/')
            WebDriverWait(browser, 60).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '#container li'))
            )

            # 转化date_type，再检查该页面有没有date_type
            pass

            # 跳转页面
            browser.execute_script("zl_pp('2018.07.03','ip')")

            # 选择每页10条
            WebDriverWait(browser, 60).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '.list_dl select'))
            )
            browser.find_element_by_css_selector('.list_dl option[value=\'10\']').click()

            # 跳转到crawl_state对应页面的下一页
            if crawl_state != 1:
                WebDriverWait(browser, 60).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '.w50'))
                )
                browser.find_element_by_css_selector('.w50').send_keys(crawl_state)
                browser.find_element_by_css_selector('.w50').send_keys(Keys.ENTER)

                # 等待页面加载
                WebDriverWait(browser, 60).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '.cp_linr'))
                )

            return browser

        except TimeoutException:
            print('to_index, TimeoutException')
            browser.refresh()
            self.to_index(date_type, crawl_state)

    def parse(self, browser, date_type, crawl_state1):
        crawl_state = crawl_state1
        try:
            # 提取最大页
            max_page = 0
            a_items = browser.find_elements_by_css_selector('.next a')
            for i in a_items:
                res = re.search('\d+', i.text)
                if res is not None:
                    page = int(res.group())
                    if page > max_page:
                        max_page = page

            while crawl_state <= max_page:
                # 解析当前页面
                self.parse_detail(browser, date_type, str(crawl_state))
                # 判断是否有下一页,有就进入
                if crawl_state+1 <= max_page:
                    browser.find_element_by_css_selector('.w50').send_keys(crawl_state)
                    browser.find_element_by_css_selector('.w50').send_keys(Keys.ENTER)
                    WebDriverWait(browser, 60).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, '.cp_linr'))
                    )
                crawl_state += 1

            # while结束，说明爬完了，设置成已爬完
            self.set_crawl_state(date_type, '已爬')

        except TimeoutException:
            print('parse(),TimeoutException')
            self.to_index(date_type, crawl_state)
            self.parse(browser, date_type, crawl_state)

    def parse_detail(self, browser, date_type, crawl_state):
        # crawl_state 是当前页
        s1 = lxml.html.fromstring(browser.page_source)

        title = ''
        abstract = ''
        published_application_num = ''
        published_application_date = ''
        published_num = ''
        published_date = ''
        applicant = ''
        inventor = ''
        address = ''
        sort_num = ''
        agency = ''
        agent = ''

        items = s1.cssselect('.cp_box')
        for i in items:
            title_css = i.cssselect('.cp_linr h1')
            if title_css:
                title = title_css[0].text.strip()

            abstract_css = i.cssselect('.cp_jsh')
            if abstract_css:
                abstract = abstract_css[0].text_content().replace('\t', '').replace('\n', '')
                res = re.search('摘要：(.*)全部', abstract)
                if res is not None:
                    abstract = res.group(1)

            li_items = i.cssselect('.cp_linr>ul li')
            for j in li_items:
                text = j.text
                if not isinstance(text, str):
                    continue

                p_a_n_res = re.search('申请公布号：(.*)', text)
                if p_a_n_res is not None:
                    published_application_num = p_a_n_res.group(1)

                p_a_d_res = re.search('申请公布日：(.*)', text)
                if p_a_d_res is not None:
                    published_application_date = p_a_d_res.group(1)

                p_n_res = re.search('申请号：(.*)', text)
                if p_n_res is not None:
                    published_num = p_n_res.group(1)

                p_d_res = re.search('申请日：(.*)', text)
                if p_d_res is not None:
                    published_date = p_d_res.group(1)

                a_res = re.search('申请人：(.*)', text)
                if a_res is not None:
                    applicant = a_res.group(1)

                i_res = re.search('发明人：(.*)', text)
                if i_res is not None:
                    inventor = i_res.group(1)

                address_res = re.search('地址：(.*)', text)
                if address_res is not None:
                    address = address_res.group(1)

                s_n_res = re.search('分类号：(.*)', text)
                if s_n_res is not None:
                    sort_num = s_n_res.group(1)

                agency_res = re.search('专利代理机构：(.*)', text)
                if agency_res is not None:
                    agency = agency_res.group(1)

                agent_res = re.search('代理人：(.*)', text)
                if agent_res is not None:
                    agent = agent_res.group(1)

            gmt_create = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            gmt_modified = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # 将数据保存在 insert_list
            insert_list = [
                title, abstract, published_application_num, published_application_date, published_num, published_date,
                applicant, inventor, address, sort_num, agency, agent, gmt_create, gmt_modified
            ]
            Crawl.insert_list.extend(insert_list)

            Crawl.crawl_num += 1
            print('爬取了' + str(Crawl.crawl_num) + '个')

        # 插入数据库
        self.insert_db(date_type, crawl_state)

    def set_crawl_state(self, date_type, crawl_state):
        """
        保存当前爬取状态
        :param date_type:
        :param crawl_state:
        :return:
        """
        sql_update = "UPDATE is_crawl SET crawl_state = '{0}' WHERE date_type = '{1}'".format(crawl_state, date_type)
        db = MySQLdb.connect(
            Crawl.MYSQL_HOST,
            Crawl.MYSQL_USER,
            Crawl.MYSQL_PASSWORD,
            Crawl.MYSQL_DBNAME,
            charset='utf8',
            use_unicode=True
        )
        cursor = db.cursor()
        cursor.execute(sql_update)
        db.commit()
        db.close()

    def insert_db(self, date_type, crawl_state):
        """
        保存 insert_list 的数据,并保存当前爬取到哪一页
        :param date_type:
        :param crawl_state:
        :return:
        """
        # 保存 insert_list 的数据
        n = int(len(Crawl.insert_list) / 14)

        insert_1 = """
            insert into literature(title, abstract, published_application_num, published_application_date, 
            published_num, published_date, applicant, inventor, address, sort_num, agency, agent, gmt_create, 
            gmt_modified) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        insert_2 = ''
        if n > 1:
            for i in range(0, n - 1):
                insert_2 = insert_2 + ', (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        insert_3 = """ ON DUPLICATE KEY UPDATE gmt_modified=VALUES(gmt_modified)"""
        insert_sql = insert_1 + insert_2 + insert_3

        db = MySQLdb.connect(
            Crawl.MYSQL_HOST,
            Crawl.MYSQL_USER,
            Crawl.MYSQL_PASSWORD,
            Crawl.MYSQL_DBNAME,
            charset='utf8',
            use_unicode=True
        )
        cursor = db.cursor()
        cursor.execute(insert_sql, tuple(Crawl.insert_list))
        print('插入' + str(n) + '个数据,受影响行数：', end='')
        print(cursor.rowcount, '\n')
        db.commit()
        db.close()
        Crawl.insert_list.clear()

        # 保存当前爬取到哪个日期
        self.set_crawl_state(date_type, crawl_state)

if __name__ == '__main__':
    is_crawl = 0
    while is_crawl == 0:
        try:
            zhuanli_crawl = Crawl()
            is_crawl = zhuanli_crawl.start()
        except Exception as e:
            print(e.args)
            zhuanli_crawl.browser.quit()

    # zhuanli_crawl = Crawl()
    # is_crawl = zhuanli_crawl.start()