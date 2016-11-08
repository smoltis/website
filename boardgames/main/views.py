from django.shortcuts import render, redirect


def home(request):
    if request.user.is_authenticated():
        return redirect('user/home')
    return render(request, "main/home.html")