# order of functions
# 1. collectDocuments(path) -> Collect the documents to be indexed.
#       1.01 tokenization(document) -> Tokenize the text.
#   1.1 Index the documents that each term occurs in
# 2. processingQuery(query) -> taking query input
#       2.02 postfixQuery(query_stream) -> converting infix boolean query to postfix
#       2.03 postfixEvaluation()
#           2.03.1 intersection and union of postings lists

import os
import glob
import re
from contractions import contractions
from porterstemmer import PorterStemmer
from stack import Stack

sizes_of_stemmed_lists = []
term_document_dictionary = {}
list_of_documentID = []
query_dictionary = {}

#collecting documents
def collectDocuments(path):
    # opening files one by one in the directory
    for filename in glob.glob(os.path.join(path, '*.txt')):
        with open(os.path.join(os.getcwd(), filename), 'r') as f:
            #handling the encoding of the text files
            f = open(filename, "r", encoding="utf8")
            #Reading from file
            file_contents = f.read()
            f.close()

            # sending the document for tokenization
            final_tokenized_terms = []
            final_tokenized_terms = tokenization(file_contents)

            #extracting documentid from the path
            path_split = os.path.split(filename)
            document_id = path_split[1][:-4]
            integer_document_id = int(document_id)
            #saving document id
            list_of_documentID.append(integer_document_id)
            #maintaining dictionary with posting list
            for word in final_tokenized_terms:
                if word in term_document_dictionary.keys():
                    #if key exists then append the document list
                    term_document_dictionary[word].append(integer_document_id)
                    #sorting the postings list after a new posting is entered
                    term_document_dictionary[word].sort()
                else:
                    #if key does not exist then create a document list for the key
                    term_document_dictionary[word] = [integer_document_id]

#tokenizing
def tokenization(document):
    #CASEFOLDING
    # casefolding or lower casing the whole document
    document_to_work = document.casefold()

    #OPENING CONTRACTIONS
    # making a list for contractions processing
    list_terms = document_to_work.split()
    list_terms_to_work = list()
    for word in list_terms:
        if word in contractions:
            #using an imported dictionary for contractions
            list_terms_to_work.append(contractions[word])
        else:
            list_terms_to_work.append(word)

    #REMOVING PUNCTUATIONS
    document_to_work = ' '.join(list_terms_to_work)
    document_to_work = re.sub(r'[^\w\s]', '', document_to_work)

    #REMOVING FINAL LEFT OVER WHITESPACES
    finalised_terms_with_stopwords = document_to_work.split()
    # REMOVING STOP WORDS AND DUPLICATE WORDS
    # opening stopwords file
    f = open('dataset/Stopword-List.txt', 'r', encoding='utf8')
    stop_words = f.read()
    stop_list = stop_words.split()
    f.close()
    #removing stop words
    finalised_terms_without_stopwords = list(set(finalised_terms_with_stopwords)^set(stop_list))
    finalised_terms_without_stopwords = list(finalised_terms_without_stopwords)

    #STEMMING
    # stemmer = PorterStemmer()
    # stem_list = list()
    # stem_list = [stemmer.stem(word) for word in finalised_terms_without_stopwords]
    # #removing stemmed duplicates
    # stem_list = set(stem_list)
    # stem_list = list(stem_list)

    #SORTING THE TERMS
    # stem_list.sort()
    # sizes_of_stemmed_lists.append(len(stem_list))
    return finalised_terms_without_stopwords

#Query processing
def processingQuery(query):
    answer = []
    postfixed_query = list()
    #LOWER CASE HANDLING AND LISTING TERMS IN A QUERY
    query_stream = query.lower().split()
    # print(query_stream)
    #not a proximity query
    if len(query_stream) == 1:
        if query_stream[0] in term_document_dictionary.keys():
            answer = term_document_dictionary[query_stream[0]]
        return answer
    elif len(query_stream) > 1:
        if "/" not in query:
            #postfixing an infix boolean query
            postfixed_query = postfixQuery(query_stream)
            # print(postfixed_query)
            answer = evaluatePostfixQuery(postfixed_query)
        else:
            #handling proximity query
            if "and" not in query_stream:
                query=" + ".join(query_stream)
                # print(query)
                query_stream_positional=query.split()
            proximity = query_stream_positional.pop()
            query_stream_positional.pop()
            # print(query_stream_positional)
            proximity = proximity.replace("/","")
            proximity_len = int(proximity) + 1
            answer = evaluatePostfixQuery(query_stream_positional)
            query_stream.pop()
            prox_answer = []
            for document in answer:
                #checking proximity
                path = 'dataset/ShortStories/' + str(document)+'.txt'
                f = open(path, "r", encoding="utf8")
                # Reading from file
                file_contents = f.read()
                f.close()
                #fixing punctuations
                file_contents = re.sub(r'[^\w\s]', '', file_contents)
                #lower casing
                terms = file_contents.split()
                terms = [i.lower() for i in terms]
                # 2 term proximity query
                if len(query_stream) == 2:
                    index_1 = []
                    index_2 = []
                    count_1 = int(0)
                    for item in terms:
                        if item == query_stream[0]:
                            index_1.append(count_1)
                        if item == query_stream[1]:
                            index_2.append(count_1)
                        count_1 += 1
                    for item in index_1:
                        i = int(1)
                        while i != proximity_len + 1:
                            if item + i in index_2:
                                prox_answer.append(document)
                            elif item - i in index_2:
                                prox_answer.append(document)
                            i += 1
                # 3 term proximity query
                else:
                    index_1 = []
                    index_2 = []
                    index_3 = []
                    count_1 = int(0)
                    for item in terms:
                        if item == query_stream[0]:
                            index_1.append(count_1)
                        if item == query_stream[1]:
                            index_2.append(count_1)
                        if item == query_stream[2]:
                            index_3.append(count_1)
                        count_1 += 1
                    for item in index_1:
                        i = int(0)
                        while i != proximity_len + 1:
                            if item + i in index_2:
                                prox_answer.append(document)
                            elif item - i in index_2:
                                prox_answer.append(document)
                            i += 1
                answer = prox_answer
        return answer

#converting logical infix to postfix
def postfixQuery(query_stream):
    temp_stream = []
    for i in query_stream:
        if i == "not":
            temp_stream.append(i.replace("not", "-"))
        elif i == "and":
            temp_stream.append(i.replace("and", "*"))
        elif i == "or":
            temp_stream.append(i.replace("or", "+"))
        else:
            temp_stream.append(i)
    query_stream = temp_stream
    precedence = {"-": 3, "*": 2, "+": 1}
    postfix_query = []
    stack = Stack()
    for term_token in query_stream:
        if term_token.isidentifier():
            postfix_query.append(term_token)
        elif term_token == '(':
            stack.push(term_token)
        elif term_token == ')':
            topToken = stack.pop()
            while topToken != '(':
                postfix_query.append(topToken)
                topToken = stack.pop()

        else:  # must be operator
            if not stack.empty():
                while not stack.empty() and precedence[stack.peek()] >= precedence[term_token]:
                    postfix_query.append(stack.pop())

            stack.push(term_token)

    while not stack.empty():
        postfix_query.append(stack.pop())

    return postfix_query

#evaluating the postfix expression
def evaluatePostfixQuery(postfixed_query):
    stack = Stack()
    operators = ["*","+","-"]
    for token in postfixed_query:
        if token not in operators:
            stack.push(token)
        else:
            if token == "-":
                term_1 = stack.pop()
                # print(term_1)
                if term_1 in term_document_dictionary.keys():
                    not_term_1 = list(set(list_of_documentID)^set(term_document_dictionary[term_1]))
                    query_dictionary[term_1] = not_term_1
                else:
                    query_dictionary[term_1] = []
                stack.push(term_1)
            elif token == "*":
                term_2 = stack.pop()
                term_1 = stack.pop()
                # print(term_1, term_2)
                if term_1 not in query_dictionary.keys():
                    if term_1 in term_document_dictionary.keys():
                        query_dictionary[term_1] = term_document_dictionary[term_1]
                    else:
                        query_dictionary[term_1] = []
                if term_2 not in query_dictionary.keys():
                    if term_2 in term_document_dictionary.keys():
                        query_dictionary[term_2] = term_document_dictionary[term_2]
                    else:
                        query_dictionary[term_2] = []
                query_dictionary["answer"] = intersect(term_1,term_2)
                stack.push("answer")
            elif token == "+":
                term_2 = stack.pop()
                term_1 = stack.pop()
                # print(term_1, term_2)
                if term_1 not in query_dictionary.keys():
                    if term_1 in term_document_dictionary.keys():
                        query_dictionary[term_1] = term_document_dictionary[term_1]
                    else:
                        query_dictionary[term_1] = []
                if term_2 not in query_dictionary.keys():
                    if term_2 in term_document_dictionary.keys():
                        query_dictionary[term_2] = term_document_dictionary[term_2]
                    else:
                        query_dictionary[term_2] = []
                query_dictionary["answer"] = union(term_1, term_2)
                stack.push("answer")
    return query_dictionary["answer"]

#intersection of two postings lists
def intersect(term_1,term_2):
    answer = []
    postings_term_1 = query_dictionary[term_1]
    postings_term_2 = query_dictionary[term_2]
    len_p1 = len(postings_term_1)
    len_p2 = len(postings_term_2)
    index_p1 = int(0)
    index_p2 = int(0)
    while len_p1 != 0 and len_p2 != 0:
        if index_p1 <len(postings_term_1) and index_p2<len(postings_term_2):
            if postings_term_1[index_p1] == postings_term_2[index_p2]:
                answer.append(postings_term_1[index_p1])
                index_p1 += 1
                len_p1 -= 1
                index_p2 += 1
                len_p2 -= 1
            else:
                if postings_term_1[index_p1] < postings_term_2[index_p2]:
                    index_p1 += 1
                    len_p1 -= 1
                else:
                    index_p2 += 1
                    len_p2 -= 1

    return answer

#union of two postings lists
def union(term_1,term_2):
    answer = []
    postings_term_1 = query_dictionary[term_1]
    postings_term_2 = query_dictionary[term_2]
    answer = [term for term in postings_term_1]
    answer = [term for term in postings_term_2] + answer
    answer = set(answer)
    answer = list(answer)
    answer.sort()
    return answer


if __name__ == '__main__':
    collectDocuments('dataset/ShortStories')
    print("Size of the dictionary: "+ str(len(term_document_dictionary)))
    q = "god and man and love"
    print("Query: ",q)
    print(processingQuery(q))



