# Managing Big Data Project
**Dataset:** [Amazon_dataset 2018](https://cseweb.ucsd.edu/~jmcauley/datasets/amazon_v2/) \
**Path:** `/user/s2773430/data/Amazon_2018` \
**Size on HDFS:** `45.1 G`  **Replicated size on HDFS:** `135.4 G`
## Dataset schema
### Review Dataset schema
```csv
root
 |-- overall: double (nullable = true)
 |-- verified: boolean (nullable = true)
 |-- reviewTime: string (nullable = true)
 |-- reviewerID: string (nullable = true)
 |-- asin: string (nullable = true)
 |-- style: struct (nullable = true)
 |    |-- Size: string (nullable = true)
 |    |-- style name: string (nullable = true)
 |-- reviewerName: string (nullable = true)
 |-- reviewText: string (nullable = true)
 |-- summary: string (nullable = true)
 |-- unixReviewTime: long (nullable = true)
```
### Metadata Dataset schema
```csv
root
 |-- category: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- tech1: string (nullable = true)
 |-- description: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- fit: string (nullable = true)
 |-- title: string (nullable = true)
 |-- also_buy: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- tech2: string (nullable = true)
 |-- brand: string (nullable = true)
 |-- feature: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- rank: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- also_view: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- main_cat: string (nullable = true)
 |-- similar_item: string (nullable = true)
 |-- date: string (nullable = true)
 |-- price: string (nullable = true)
 |-- asin: string (nullable = true)
 |-- imageURL: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- imageURLHighRes: array (nullable = true)
 |    |-- element: string (containsNull = true)
```
### Joined Dataset schema
```csv
root
 |-- asin: string (nullable = true)
 |-- category: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- tech1: string (nullable = true)
 |-- description: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- fit: string (nullable = true)
 |-- title: string (nullable = true)
 |-- also_buy: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- tech2: string (nullable = true)
 |-- brand: string (nullable = true)
 |-- feature: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- rank: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- also_view: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- main_cat: string (nullable = true)
 |-- similar_item: string (nullable = true)
 |-- date: string (nullable = true)
 |-- price: string (nullable = true)
 |-- imageURL: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- imageURLHighRes: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- reviews: array (nullable = true)
 |    |-- element: struct (containsNull = false)
 |    |    |-- overall: double (nullable = true)
 |    |    |-- verified: boolean (nullable = true)
 |    |    |-- reviewTime: string (nullable = true)
 |    |    |-- reviewerID: string (nullable = true)
 |    |    |-- style: struct (nullable = true)
 |    |    |    |-- Size: string (nullable = true)
 |    |    |    |-- style name: string (nullable = true)
 |    |    |-- reviewerName: string (nullable = true)
 |    |    |-- reviewText: string (nullable = true)
 |    |    |-- summary: string (nullable = true)
 |    |    |-- unixReviewTime: long (nullable = true)
```
## Dataset citation
```bibtex
@inproceedings{ni-etal-2019-justifying,
    title = "Justifying Recommendations using Distantly-Labeled Reviews and Fine-Grained Aspects",
    author = "Ni, Jianmo  and
      Li, Jiacheng  and
      McAuley, Julian",
    editor = "Inui, Kentaro  and
      Jiang, Jing  and
      Ng, Vincent  and
      Wan, Xiaojun",
    booktitle = "Proceedings of the 2019 Conference on Empirical Methods in Natural Language Processing and the 9th International Joint Conference on Natural Language Processing (EMNLP-IJCNLP)",
    month = nov,
    year = "2019",
    address = "Hong Kong, China",
    publisher = "Association for Computational Linguistics",
    url = "https://aclanthology.org/D19-1018",
    doi = "10.18653/v1/D19-1018",
    pages = "188--197",
    abstract = "Several recent works have considered the problem of generating reviews (or {`}tips{'}) as a form of explanation as to why a recommendation might match a customer{'}s interests. While promising, we demonstrate that existing approaches struggle (in terms of both quality and content) to generate justifications that are relevant to users{'} decision-making process. We seek to introduce new datasets and methods to address the recommendation justification task. In terms of data, we first propose an {`}extractive{'} approach to identify review segments which justify users{'} intentions; this approach is then used to distantly label massive review corpora and construct large-scale personalized recommendation justification datasets. In terms of generation, we are able to design two personalized generation models with this data: (1) a reference-based Seq2Seq model with aspect-planning which can generate justifications covering different aspects, and (2) an aspect-conditional masked language model which can generate diverse justifications based on templates extracted from justification histories. We conduct experiments on two real-world datasets which show that our model is capable of generating convincing and diverse justifications.",
}
```
