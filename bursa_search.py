import multiprocessing
import math
import os
import requests
import sys
import pandas as pd
import urllib
import datetime
import random
import json

no_of_pool = 10

cwd = os.path.dirname(__file__)
tmp = os.path.join(cwd, 'tmp')
os.makedirs(tmp, exist_ok=True)

#bursa_company_xl = os.path.join(tmp, 'bursa_company_choices.xlsx')
#bursa_category_xl = os.path.join(tmp, 'bursa_cat_choices.xlsx')
#bursa_subcategory_xl = os.path.join(tmp, 'bursa_sub_type_choices.xlsx')
#bursa_category_tree_xl = os.path.join(tmp, 'bursa_cat_trees.xlsx')
#bursa_market_xl = os.path.join(tmp, 'bursa_mkt_choices.xlsx')
#bursa_sector_xl = os.path.join(tmp, 'bursa_sec_choices.xlsx')
#bursa_subsector_xl = os.path.join(tmp, 'bursa_subsec_choices.xlsx')
#bursa_market_tree_xl = os.path.join(tmp, 'bursa_mkt_trees.xlsx')

#company_choices = pd.read_excel(bursa_company_xl)
#cat_choices = pd.read_excel(bursa_category_xl)
#sub_type_choices = pd.read_excel(bursa_subcategory_xl)
#cat_trees = pd.read_excel(bursa_category_tree_xl)
#mkt_choices = pd.read_excel(bursa_market_xl)
#sec_choices = pd.read_excel(bursa_sector_xl)
#subsec_choices = pd.read_excel(bursa_subsector_xl)
#mkt_trees = pd.read_excel(bursa_market_tree_xl)

def retrieve_api(url):
    print('url : %s' % url)
    r = requests.get(url)
    assert r.ok
    data = r.json()
    try:
        path = data['data'][0][0]
        path = '%s.json' % path
    except IndexError:
        now = datetime.datetime.now()
        rand = str(random.randint(0, 1000))
        path = now.strftime('%Y%m%s_%H%M%S') + ('_%s.json' % rand)
    path = os.path.join(tmp, path)
    with open(path, 'w') as f:
        json.dump(data,f)
    return data
    
    
        
    
    
class Bursa_Search:
    search_url = '''https://www.bursamalaysia.com/api/v1/announcements/search'''
    columns = ['id', 'date_raw', 'company_raw', 'ann_raw']
    
    ann_type: str = 'company'
    company: str = ''
    keyword: str = ''
    cat: str = ''
    sub_type: str = ''
    mkt: str = ''
    sec: str = ''
    subsec: str = ''
    per_page : int = 20
    page : int = 1
    
    def __init__(
        self,
        company: str = None,
        keyword: str = None,
        dt_ht: str = None,
        dt_lt: str = None,
        cat: str = None,
        sub_type: str = None,
        mkt: str = None,
        sec: str = None,
        subsec: str = None,
        per_page : int = 20,
        page : int = 1,
        get_all : bool = True,
        ):
        
        self.company = company or ''
        self.keyword = keyword or ''
        self.dt_ht = dt_ht or ''
        self.dt_lt = dt_lt or ''
        self.cat = cat or ''
        self.sub_type = sub_type or ''
        self.mkt = mkt or ''
        self.sec = sec or ''
        self.subsec = subsec or ''
        self.get_all = bool(get_all)
        
        if self.get_all:
            self.page = 1
            
        try:
            self.per_page = per_page or 10
            self.per_page = int(self.per_page)
            assert self.per_page >= 10
            assert self.per_page <= 20
        except Exception:
            self.per_page = 20
        try:
            self.page = page or 1
            self.page = int(self.page)
            assert self.page > 0
        except Exception:
            self.page = 1
        
        self._records = []
        self._count = 0
        self._maxcount = 0
        
        params = self.get_params()
        result = self.search(params=params)
        self._records = self.resolve_data(result)
        self._maxcount = self.resolve_maxcount(result)
        self._count = len(self._records)
        
        if get_all:
            maxpage = math.ceil(self._maxcount/self.per_page)
            if self.page < maxpage:
                params_ls = [ params.copy() | {'page': i}  for i in range(2, maxpage+1) ]
                url_ls = [ self._build_url(params=x) for x in params_ls]
                pool = multiprocessing.Pool(processes=no_of_pool)
                pool_results = pool.map(retrieve_api, url_ls)
                pool.close()
                pool.join()
                pool_results = [ self.resolve_data(r) for r in pool_results ]
                pool_results.sort(key=lambda x : x[0][0])
                for result in pool_results:
                    self._records.extend(result)
                self._count = len(self._records)
        
    def get_params(self):
        return {
            'ann_type': self.ann_type,
            'company': self.company,
            'keyword': self.keyword,
            'dt_ht' : self.dt_ht.replace('-', '/'),
            'dt_lt' : self.dt_lt.replace('-', '/'),
            'cat': self.cat,
            'sub_type': self.sub_type,
            'mkt': self.mkt,
            'sec': self.sec,
            'subsec': self.subsec,
            'per_page': self.per_page,
            'page': self.page,
        }
        
    def save(self, path : str = None):
        path = path or os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output.xlsx')
        self.dataframe.to_excel(path, index = False)
    
    @property
    def records(self):
        return self._records
    
    @property
    def count(self):
        return self._count
    
    @property
    def maxcount(self):
        return self._maxcount
    
    @property
    def dataframe(self):
        if not hasattr(self, '_dataframe'):
            self._dataframe = pd.DataFrame(self.records, columns=self.columns)
        return self._dataframe
    
    def _build_url(self, params: dict = None):
        if params:
            query_string = urllib.parse.urlencode(params)
        else:
            query_string = ''
        url = self.search_url + '?' + query_string
        return url
    
    def search(self, params: dict = None):
        params = params or self.get_params()
        params['ann_type'] = self.ann_type
        url = self._build_url(params=params)
        return retrieve_api(url)
    
    def resolve_data(self, data: dict):
        return data.get('data', [])
    
    def resolve_maxcount(self, data: dict):
        return data.get('recordsTotal', 0)


def run_script(*args):
    conf = {
        'company': None,
        'keyword': None,
        'dt_ht': None,
        'dt_lt': None,
        'cat': None,
        'sub_type': None,
        'mkt': None,
        'sec': None,
        'subsec': None,
        'per_page': None,
        'page': None,
        'get_all': False,
    }
    output = None
    params = [ '--%s' % x for x in conf]
    if args:
        try:
            i = 0
            while(i < len(args)):
                if args[i] in params:
                    param = args[i][2:]
                    if param == 'get_all':
                        conf['get_all'] = True
                        i += 1
                    else:
                        conf[param] = args[i+1]
                        i += 2
                elif args[i] == '--output':
                    output = args[i+1]
                    i += 2
                else:
                    raise SyntaxError     
        except IndexError:
            raise SyntaxError     
    query = Bursa_Search(**conf)
    query.save(path = output)


if __name__ == "__main__":
    try:
        run_script(*sys.argv[1:])
    except SyntaxError:
        name = os.path.basename(__file__)
        print(
            """
            Syntaxa Error. Check the usage information below:
            
            Usage: python %s.py [parameters] 
            Parameters (Optional):
            --company <company>     Filtering on company
            --keyword <keyword>     Filtering on keyword
            --dt_ht <dt_ht>         Filtering on Date From (ie: 31-12-2022)
            --dt_lt <dt_lt>         Filtering on Date To (ie: 31-12-2022)
            --cat <cat>             Filtering on Category
            --sub_type <sub_type>   Filtering on Subcategory
            --mkt <mkt>             Filtering on Market
            --sec <sec>             Filtering on Sector
            --subsec <subsec>       Filtering on Subsector
            --per_page <per_page>   Specify records per page
            --page <page>           Specify which page
            --get_all               Get records from all pages
            --output <output>       Excel output path
            """ % name
        )
    sys.exit()
