#include <bits/stdc++.h>
#include <mpi.h>

using namespace std;

//... To compile: mpic++ phonebook-search.cpp -o phonebook-search
//... To run: mpirun -np 4 ./phonebook-search phonebook1.txt phonebook2.txt

void send_string(string text, int receiver)
{
    int length = text.size() + 1;
    MPI_Send(&length, 1, MPI_INT, receiver, 1, MPI_COMM_WORLD);
    MPI_Send(&text[0], length, MPI_CHAR, receiver, 1, MPI_COMM_WORLD);
}

string receive_string(int sender)
{
    int length;
    MPI_Recv(&length, 1, MPI_INT, sender, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    char *text = new char[length];
    MPI_Recv(text, length, MPI_CHAR, sender, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    return string(text);
}

string vector_to_string(vector<string> &words, int start, int end)
{
    string text = "";
    for (int i = start; i < min((int)words.size(), end); i++)
    {
        text += words[i] + "\n";
    }
    return text;
}

vector<string> string_to_vector(string text)
{
    stringstream x(text);
    vector<string> words;
    string word;
    while (x >> word)
    {
        words.push_back(word);
    }
    return words;
}

bool check(string& name, string& searchName){
    if (name.find(searchName) != string::npos) {
        return true;
    }
    return false;
}

void read_phonebook(vector<string> &file_names, vector<string> &ids, vector<string> &names, vector<string> &phone_numbers)
{
    int cnt = 100;
    for (const string &file : file_names) {
        ifstream f(file);
        string line;
        while (getline(f, line)) {
            if (line.empty()) continue;
            int comma = line.find(",");
            if (comma == string::npos) continue;
            // contacts.push_back({line.substr(1, comma - 2), line.substr(comma + 2, line.size() - comma - 3)});
            ids.push_back(line.substr(1, comma - 2));
            line = line.substr(comma + 1, line.size() - comma);
            comma = line.find(",");
            if (comma == string::npos) continue;
            names.push_back(line.substr(1, comma - 2));
            phone_numbers.push_back(line.substr(comma + 2, line.size() - comma - 3));
        }
        // cnt--;
        if(cnt < 0) break;
    }
}

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);

    int world_size, world_rank;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    string search;
    double start_time, finish_time;

    map<pair<string,string>, string>ultimateResult;


    if (!world_rank)
    {
        vector<string>ids, names, phone_numbers;
        vector<string> file_names(argv + 1, argv + argc);

        search = file_names.back();
        file_names.pop_back();
        cout << search << endl;
       
        read_phonebook(file_names, ids, names, phone_numbers);
        // for (int i = 0; i < 10; i++) {
        //     cout << ids[i] << " " << names[i] << " " << phone_numbers[i] << endl;
        // }
        int segment = names.size() / world_size + 1;
        // cout << "Give name to search: ";
        // cin >> search;
        for (int i = 1; i < world_size; i++)
        {
            int start = i * segment, end = start + segment;
            string ids_to_send = vector_to_string(ids, start, end);
            send_string(ids_to_send, i);
            string names_to_send = vector_to_string(names, start, end);
            send_string(names_to_send, i);
            string phone_numbers_to_send = vector_to_string(phone_numbers, start, end);
            send_string(phone_numbers_to_send, i);
            send_string(search, i);
        }
        start_time = MPI_Wtime();
        for(int i = 0 ; i < segment; i++){
            bool isMatched = check(names[i], search);
            if(isMatched){
                ultimateResult[{ids[i], names[i]}] = phone_numbers[i];
            }
        }
        finish_time = MPI_Wtime();
    }
    else
    {
        string received_ids = receive_string(0);
        vector<string> ids = string_to_vector(received_ids);
        string received_names = receive_string(0);
        vector<string> names = string_to_vector(received_names);
        string received_phone_numbers = receive_string(0);
        vector<string> phone_numbers = string_to_vector(received_phone_numbers);
        search = receive_string(0);

        string matchedId = "";
        string matchedName = "";
        string matchedNumber = "";

        start_time = MPI_Wtime();
        for(int i = 0; i < names.size(); i++){
            bool isMatched = check(names[i], search);
            if(isMatched){
                matchedId += ids[i] + " ";
                matchedName += names[i] + " ";
                matchedNumber += phone_numbers[i] + " ";
            }
        }
        finish_time = MPI_Wtime();
        
        send_string(matchedId, 0);
        send_string(matchedName, 0);
        send_string(matchedNumber, 0);
    }

    if(world_rank == 0){
        string allMatchedName = "";
        string allMatchedNumber = "";
        string allMatchedId = "";

        for(int i = 1; i < world_size; i++){
            allMatchedId += receive_string(i);
            allMatchedName += receive_string(i);
            allMatchedNumber += receive_string(i);
        }
        stringstream ss1(allMatchedName), ss2(allMatchedNumber), ss3(allMatchedId);
        string word1, word2, word3;
        while(ss3 >> word3 && ss1 >> word1 && ss2 >> word2){
            ultimateResult[{word3, word1}] = word2;
            // ultimateResult.push_back({word1, word2});
        }
        ofstream out("output.txt");
        for(auto [key, val] : ultimateResult){
            out << key.first << " " << key.second << "  " << val << endl;
        }
        out.close();
    }

    MPI_Barrier(MPI_COMM_WORLD);

    printf("Process %d took %f seconds.\n", world_rank, finish_time - start_time);

    MPI_Finalize();

    return 0;
}