from django.shortcuts import render,redirect
from .forms import SearchForm
from elasticsearch import Elasticsearch

class Struct:
    def __init__(self, **entries): 
        self.__dict__.update(entries)

# Create your views here.
def home(request):

    if request.method == 'POST':
        form = SearchForm(request.POST)
        if form.is_valid():
            cd = form.cleaned_data
            word = cd['input_str']
            ES_HOST = {"host": "localhost", "port": 9200}
            es = Elasticsearch()
            res = es.search(index='kors_py',size=50,body={
                "query":{"bool":{
                        "should":[ {"match": {"PartNo": {
                                                         "query": word,
                                                         "analyzer": "standard"},
                        }},
                                   {"match": {"PartName": {"query": word,
                                                           "analyzer": "standard"}}}
                                ]}
                        }})
            found = res['hits']['total']
            decoded_data = list()
            if found>0:
                for doc in res['hits']['hits']:
                    args = doc['_source']
                    decoded_data.append(Struct(**args))

            return render(request, 'search/results.html',
                   {
                       'found': found,
                       'objects': decoded_data, 
                       'form': form
                   })
    else:
        form = SearchForm()
  
    return render(request, "search/home.html", {'form': form})