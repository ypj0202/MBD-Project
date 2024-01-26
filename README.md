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
### Statisitc per category
```linux
<-----------Statistics for category AMAZON_FASHION-------------->
Review count:883636
Joined review count:883636
Unique product in meta:186194
Joined product count:186189
Unique product in review:186189
<-----------Statistics for category All_Beauty-------------->
Review count:371345
Joined review count:366223
Unique product in meta:32488
Joined product count:32486
Unique product in review:32586
<-----------Statistics for category Appliances-------------->
Review count:602777
Joined review count:601630
Unique product in meta:30239
Joined product count:30238
Unique product in review:30252
<-----------Statistics for category Arts_Crafts_and_Sewing-------------->
Review count:2875917
Joined review count:2867249
Unique product in meta:302380
Joined product count:302372
Unique product in review:302809
<-----------Statistics for category Automotive-------------->
Review count:7990166
Joined review count:7979984
Unique product in meta:924693
Joined product count:924662
Unique product in review:925387
<-----------Statistics for category Books-------------->
Review count:51311621
Joined review count:51309031
Unique product in meta:2930024
Joined product count:2929875
Unique product in review:2930451
<-----------Statistics for category CDs_and_Vinyl-------------->
Review count:4543369
Joined review count:4153436
Unique product in meta:412325
Joined product count:412305
Unique product in review:434060
<-----------Statistics for category Cell_Phones_and_Accessories-------------->
Review count:10063255
Joined review count:10061221
Unique product in meta:589356
Joined product count:589335
Unique product in review:589534
<-----------Statistics for category Clothing_Shoes_and_Jewelry-------------->
Review count:32292099
Joined review count:32292099
Unique product in meta:2681355
Joined product count:2681297
Unique product in review:2681297
<-----------Statistics for category Digital_Music-------------->
Review count:1584082
Joined review count:164460
Unique product in meta:66013
Joined product count:66010
Unique product in review:456992
<-----------Statistics for category Electronics-------------->
Review count:20994353
Joined review count:20976794
Unique product in meta:756077
Joined product count:756061
Unique product in review:756489
<-----------Statistics for category Gift_Cards-------------->
Review count:147194
Joined review count:147190
Unique product in meta:1547
Joined product count:1547
Unique product in review:1548
<-----------Statistics for category Grocery_and_Gourmet_Food-------------->
Review count:5074160
Joined review count:5071335
Unique product in meta:283354
Joined product count:283349
Unique product in review:283507
<-----------Statistics for category Home_and_Kitchen-------------->
Review count:21928568
Joined review count:21904199
Unique product in meta:1285392
Joined product count:1285364
Unique product in review:1286050
<-----------Statistics for category Industrial_and_Scientific-------------->
Review count:1758333
Joined review count:1756292
Unique product in meta:165687
Joined product count:165682
Unique product in review:165764
<-----------Statistics for category Kindle_Store-------------->
Review count:5722988
Joined review count:5680726
Unique product in meta:491670
Joined product count:491660
Unique product in review:493849
<-----------Statistics for category Luxury_Beauty-------------->
Review count:574628
Joined review count:574338
Unique product in meta:12111
Joined product count:12111
Unique product in review:12120
<-----------Statistics for category Magazine_Subscriptions-------------->
Review count:89689
Joined review count:85219
Unique product in meta:2320
Joined product count:2320
Unique product in review:2428
<-----------Statistics for category Movies_and_TV-------------->
Review count:8765568
Joined review count:8752845
Unique product in meta:181839
Joined product count:181828
Unique product in review:182032
<-----------Statistics for category Musical_Instruments-------------->
Review count:1512530
Joined review count:1511813
Unique product in meta:112135
Joined product count:112132
Unique product in review:112222
<-----------Statistics for category Office_Products-------------->
Review count:5581313
Joined review count:5575053
Unique product in meta:306617
Joined product count:306615
Unique product in review:306800
<-----------Statistics for category Patio_Lawn_and_Garden-------------->
Review count:5236058
Joined review count:5230119
Unique product in meta:276332
Joined product count:276323
Unique product in review:276563
<-----------Statistics for category Pet_Supplies-------------->
Review count:6542483
Joined review count:6536153
Unique product in meta:198265
Joined product count:198260
Unique product in review:198402
<-----------Statistics for category Prime_Pantry-------------->
Review count:471614
Joined review count:471554
Unique product in meta:10812
Joined product count:10812
Unique product in review:10814
<-----------Statistics for category Software-------------->
Review count:459436
Joined review count:459050
Unique product in meta:21639
Joined product count:21638
Unique product in review:21663
<-----------Statistics for category Sports_and_Outdoors-------------->
Review count:12980837
Joined review count:12963012
Unique product in meta:957217
Joined product count:957192
Unique product in review:957764
<-----------Statistics for category Tools_and_Home_Improvement-------------->
Review count:9015203
Joined review count:8994464
Unique product in meta:559340
Joined product count:559328
Unique product in review:559775
<-----------Statistics for category Toys_and_Games-------------->
Review count:8201231
Joined review count:8191674
Unique product in meta:624284
Joined product count:624261
Unique product in review:624792
<-----------Statistics for category Video_Games-------------->
Review count:2565349
Joined review count:2561673
Unique product in meta:71911
Joined product count:71909
Unique product in review:71982
```
### Identified spikes
With the following conditions:
- Review count on spike day > 500
- Between 2010 and 2018
- Top product on spike day has review count > 500 (Additional condition)

Results:
```json
Total spikes:750
{'category': 'Books', 'count': 74}
{'category': 'Kindle_Store', 'count': 80}
{'category': 'Movies_and_TV', 'count': 84}
{'category': 'CDs_and_Vinyl', 'count': 43}
{'category': 'Digital_Music', 'count': 0}
{'category': 'Video_Games', 'count': 31}
{'category': 'Electronics', 'count': 29}
{'category': 'Office_Products', 'count': 25}
{'category': 'Tools_and_Home_Improvement', 'count': 30}
{'category': 'Software', 'count': 5}
{'category': 'Sports_and_Outdoors', 'count': 27}
{'category': 'Clothing_Shoes_and_Jewelry', 'count': 31}
{'category': 'Toys_and_Games', 'count': 31}
{'category': 'Automotive', 'count': 29}
{'category': 'Arts_Crafts_and_Sewing', 'count': 19}
{'category': 'Cell_Phones_and_Accessories', 'count': 16}
{'category': 'Industrial_and_Scientific', 'count': 22}
{'category': 'Home_and_Kitchen', 'count': 35}
{'category': 'Patio_Lawn_and_Garden', 'count': 35}
{'category': 'Musical_Instruments', 'count': 16}
{'category': 'Pet_Supplies', 'count': 19}
{'category': 'Grocery_and_Gourmet_Food', 'count': 24}
{'category': 'Appliances', 'count': 14}
{'category': 'All_Beauty', 'count': 1}
{'category': 'Magazine_Subscriptions', 'count': 0}
{'category': 'AMAZON_FASHION', 'count': 15}
{'category': 'Prime_Pantry', 'count': 7}
{'category': 'Luxury_Beauty', 'count': 6}
{'category': 'Gift_Cards', 'count': 2}
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
