from django.shortcuts import render,redirect
from .forms import SearchForm
from elasticsearch import Elasticsearch


# Create your views here.
def home(request):

    if request.method == 'POST':
        form = SearchForm(request.POST)
        if form.is_valid():
            cd = form.cleaned_data
            word = cd['input_str']
            ES_HOST = {"host": "localhost", "port": 9200}
            es = Elasticsearch(hosts = [ES_HOST])
            res = es.search(index='kors',size=25,body={"query":{"match_all": {}}})
            
            return render(request, "search/results.html", {'form': form)

    else:
        form = SearchForm()
  
    return render(request, "search/home.html", {'form': form)