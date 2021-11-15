#include<iostream>
#include<string>
using namespace std;
int main(int argc, char const *argv[]){
    string_view s;

    string str = "123\0 456";
    cout << str << " str size " << str.size()<<endl;
    s = str;
    cout << str << " size " << str.size()<<endl;

    string str2 = s.data();
    cout << str2 << " str2 size " << str2.size();
    return 0;
}

