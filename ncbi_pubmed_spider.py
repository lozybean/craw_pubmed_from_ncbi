#!/usr/bin/env python
# -*- coding: utf-8 -*- \#
"""
@author = 'liangzb'
@date = '2016-03-16'

"""

import os
import sys
import json
import time
import argparse
import re
from urllib import request
from multiprocessing import Pool
from collections import OrderedDict, defaultdict
from bs4 import BeautifulSoup
from socket import timeout
from urllib.error import URLError
from http.client import IncompleteRead
from lib.util import f


class SafeSub(dict):
    def __missing__(self, key):
        return '{' + key + '}'


def f(text, mapping=None):
    if mapping is None:
        text = text.format_map(SafeSub(sys._getframe(1).f_locals))
        return text.format_map(SafeSub(sys._getframe(1).f_globals))
    elif isinstance(mapping, dict):
        return text.format_map(SafeSub(mapping))
    else:
        return text.format_map(SafeSub(vars(mapping)))


def read_params():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-r', '--rs_file', dest='rs_file',
                        metavar='FILE', type=str, required=True,
                        help="set the rs file, "
                             "with one rs_num per line")
    parser.add_argument('-o', '--out_file', dest='out_file',
                        metavar='FILE', type=str, default='./output.txt',
                        help="set the output file")
    parser.add_argument('-t', '--threading', dest='threading',
                        metavar='INT', type=int, default=20,
                        help="how many threads will you use")
    args = parser.parse_args()
    return args


def get_request(url):
    req = request.Request(url=url, method='GET')
    req.add_header(
        'Content-Type',
        f(
            'application/x-www-form-urlencoded;'
            'charset=utf-8'
        )
    )
    req.add_header(
        'User-Agent',
        f(
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_0) '
            'AppleWebKit/601.1.56 (KHTML, like Gecko) '
            'Version/9.0 Safari/601.1.56'
        )
    )
    return req


def get_pubmed_from_list_page(html_text):
    result_list = []
    for rprt in html_text.find_all('div', class_='rprt'):
        rslt_content = OrderedDict()
        rslt = rprt.find('div', class_='rslt')
        try:
            title = rslt.find('p', class_='title').find('a')
            href = title['href']
            rslt_content['ncbi_url'] = f('http://www.ncbi.nlm.nih.gov/{href}')
            rslt_content['title'] = title.string
            supp = rslt.find('div', class_='supp')
            rslt_content['authors'] = supp.find('p', class_='desc').string
            rslt_content['source'] = ''.join(supp.find('p', class_='details').strings)
            rslt_content['report id'] = ''.join(rslt.find('dl', class_='rprtid').strings)
            rslt_content['report id'] = re.sub(r'\[\w+\]', '', rslt_content['report id'])
            result_list.append(rslt_content)
        except (TypeError, AttributeError):
            continue
    return result_list


def get_pubmed_from_single_page(html_text):
    rslt_content = OrderedDict()
    rprt_all = html_text.find('div', id='maincontent').find('div', class_='rprt_all')
    try:
        pm_id = rprt_all.find('dl', class_='rprtid').find('dd').string
        rslt_content['ncbi_url'] = f('http://www.ncbi.nlm.nih.gov/pubmed/{pm_id}')
        rslt_content['title'] = rprt_all.find('h1').string
        rslt_content['authors'] = ''.join(rprt_all.find('div', class_='auths').strings)
        rslt_content['source'] = ''.join(rprt_all.find('div', class_='cit').strings)
        rslt_content['report id'] = ''.join(rprt_all.find('dl', class_='rprtid').strings)
        rslt_content['report id'] = re.sub(r'\[\w+\]', '', rslt_content['report id'])
        return [rslt_content]
    except (TypeError, AttributeError):
        return []


def extract_ncbi(html_text):
    soap = BeautifulSoup(html_text, 'lxml')
    if (soap.find('div', id='maincontent')
                .find('div', class_='content')
                .find('div', class_='one_setting')) is not None:
        result_list = get_pubmed_from_single_page(soap)
    else:
        result_list = get_pubmed_from_list_page(soap)
    return result_list


def try_to_get_result(req):
    try_count = 0
    while 1:
        if try_count >= 10:
            return None
        try:
            with request.urlopen(req, timeout=10) as fp:
                data = fp.read()
                result_list = extract_ncbi(data)
                return result_list
        except (URLError,
                IncompleteRead,
                ConnectionResetError,
                timeout):
            try_count += 1
            print(f('error occurred, reloading {try_count} time: {url}... '))
            continue


def get_data(rs_string, sleep_second=3,
             already_dict=None,
             already_file=None):
    result_dict = {}
    if already_file is not None:
        already_dict = read_already(already_file)
    if already_dict is not None and rs_string in already_dict:
        result_dict[rs_string] = already_dict[rs_string]
    else:
        rs_id = rs_string.replace('rs', '')
        url = f(
            'http://www.ncbi.nlm.nih.gov/'
            'pubmed?Db=pubmed'
            '&DbFrom=snp&Cmd=Link'
            '&LinkName=snp_pubmed_cited'
            '&LinkReadableName=Pubmed+(SNP+Cited)'
            '&IdsFromResult={rs_id}'
        )
        req = get_request(url)
        result_dict[rs_string] = try_to_get_result(req)
        if result_dict[rs_string] is None:
            return None
    item_num = len(result_dict[rs_string])
    # sleep 3 seconds to avoid visiting too often
    time.sleep(sleep_second)
    if item_num == 0:
        result_dict[rs_string] = None
    return json.dumps(result_dict)


def output_result(result_list, out_fp):
    # out.write('rs_num\treport id\ttitle\tauthor\tsource\tncbi url\n')
    for result in result_list:
        result = result.get()
        if result is None:
            continue
        result = json.loads(result)
        print(result)
        for rs_id, publist in result.items():
            out_fp.write(f('{rs_id}'))
            if publist is None:
                out_fp.write('\t-\t-\t-\t-\t-\n')
                break
            for pubmed in publist:
                out_fp.write(f(
                    '\t'
                    '{report id}\t'
                    '{title}\t'
                    '{authors}\t'
                    '{source}\t'
                    '{ncbi_url}\n',
                    mapping=pubmed
                ))


def read_already(file_name):
    result_dict = defaultdict(list)
    rs_num = ''
    with open(file_name) as fp:
        for line in fp:
            (cur_rs_num, report_id,
             title, authors,
             source, ncbi_url) = line.rstrip().split('\t')
            if cur_rs_num:
                rs_num = cur_rs_num
            result_dict[rs_num].append(
                {
                    'ncbi_url': ncbi_url,
                    'report id': report_id,
                    'title': title,
                    'authors': authors,
                    'source': source,
                }
            )
    return result_dict


def read_rs(file_name, already_list=None):
    with open(file_name) as fp:
        for line in fp:
            rs_num = line.rstrip()
            if (rs_num and
                        already_list is not None and
                        rs_num not in already_list):
                yield rs_num


def main(rs_file, already_dict, out_fp, threading=8):
    already_list = already_dict.keys()
    pool = Pool(threading)
    result_list = []
    for rs_num in read_rs(rs_file, already_list):
        result = pool.apply_async(get_data, args=(rs_num,),
                                  kwds={'already_dict': already_dict,
                                        'sleep_second': 0})
        result_list.append(result)
        if len(result_list) >= threading * 2:
            output_result(result_list, out_fp)
            result_list = []
    output_result(result_list, out_fp)
    pool.close()
    pool.join()


def create_file(file_name):
    if not os.path.isfile(file_name):
        open(file_name, 'w').close()


if __name__ == '__main__':
    params = read_params()
    create_file(params.out_file)
    already_dict = read_already(params.out_file)
    with open(str(params.out_file), 'a') as out_fp:
        main(params.rs_file, already_dict,
             out_fp, params.threading)

