#!/usr/bin/env python
# -*- coding: utf-8 -*- \#
"""
@author = 'liangzb'
@date = '2016-03-16'

"""

import sys
import json
import time
import argparse
from urllib import request
from multiprocessing import Pool
from collections import OrderedDict
from bs4 import BeautifulSoup


class SafeSub(dict):
    def __missing__(self, key):
        return '{' + key + '}'


def f(text, mapping=None):
    """
    it is a imitation of py3.6
    """
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
                        metavar='FILE', type=str, required=True,
                        help="set the output file")
    parser.add_argument('-t', '--threading', dest='threading',
                        metavar='INT', type=int, default=20,
                        help="how many threads will you use")
    args = parser.parse_args()
    return args


def get_request(rs_id):
    req = request.Request(
        f('http://www.ncbi.nlm.nih.gov/'
          'pubmed?Db=pubmed'
          '&DbFrom=snp&Cmd=Link'
          '&LinkName=snp_pubmed_cited'
          '&LinkReadableName=Pubmed+(SNP+Cited)'
          '&IdsFromResult={rs_id}'
          ),
        method='GET'
    )
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


def extract_ncbi(html_text):
    soap = BeautifulSoup(html_text, 'lxml')
    result_list = []
    for rprt in soap.find_all('div', class_='rprt'):
        rslt_content = OrderedDict()
        rslt = rprt.find('div', class_='rslt')
        title = rslt.find('p', class_='title').find('a')
        href = title['href']
        rslt_content['ncbi_url'] = f('http://www.ncbi.nlm.nih.gov/{href}')
        rslt_content['title'] = title.string
        supp = rslt.find('div', class_='supp')
        rslt_content['authors'] = supp.find('p', class_='desc').string
        rslt_content['source'] = ''.join(supp.find('p', class_='details').strings)
        rslt_content['report id'] = ''.join(rslt.find('dl', class_='rprtid').strings)
        result_list.append(rslt_content)
    return result_list


def get_data(rs_id):
    req = get_request(rs_id)
    result_dict = {}
    sys.stderr.write('begin to crawl %s\n' % rs_id)
    with request.urlopen(req) as fp:
        data = fp.read()
        result_dict[rs_id] = extract_ncbi(data)
        item_num = len(result_dict[rs_id])
    sys.stderr.write(
        f(
            'crawl {rs_id} successful, {item_num} item returned\n'
        )
    )
    # sleep 3 seconds to avoid visiting too often
    time.sleep(3)
    if item_num:
        return json.dumps(result_dict)
    else:
        return None


def output_result(result_list, out_file):
    with open(out_file, 'w') as out:
        out.write('rs_num\treport id\ttitle\tauthor\tsource\tncbi url\n')
        for result in result_list:
            print(result)
            for rs_id, publist in result.items():
                out.write(f('{rs_id}'))
                for pubmed in publist:
                    out.write(f(
                        '\t'
                        '{report id}\t'
                        '{title}\t'
                        '{authors}\t'
                        '{source}\t'
                        '{ncbi_url}\n',
                        mapping=pubmed
                    ))


def read_rs(file_name):
    with open(file_name) as fp:
        for line in fp:
            rs_num = line.rstrip()[2:]
            if rs_num:
                yield rs_num


def main(rs_file, out_file, threading=8):
    pool = Pool(threading)
    result_list = pool.map_async(get_data, read_rs(rs_file))
    pool.close()
    pool.join()
    result_list = (
        json.loads(result)
        for result in result_list.get()
        if result is not None
    )
    output_result(result_list, out_file)


if __name__ == '__main__':
    params = read_params()
    main(params.rs_file, params.out_file, params.threading)

