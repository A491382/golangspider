package main

import (
	"context"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gohouse/gorose"
	"gopkg.in/olivere/elastic.v5"
	"log"
	"strconv"
	//"sync"
	"time"
)

type Goods struct {
	GoodsName        string
	GoodsFactory     string
	Spec             string
	Price            string
	ImageUrl         string
	Catalog          string
	SubCatalog       string
	DetailUrl        string
	CommonName       string
	LicenseNumber    string
	FitPeople        string
	Shape            string
	Brand            string
	EnglishName      string
	PinYinName       string
	Standard         string
	Looks            string
	Theory           string
	UsageRemark      string
	UntowardEffect   string
	Taboo            string
	Attention        string
	Store            string
	DrugInteractions string
}

//var waitgroup sync.WaitGroup
var DbConfig = map[string]interface{}{
	"default":         "mysql_dev",
	"SetMaxOpenConns": 300,
	"SetMaxIdleConns": 10,
	"mysql_dev": map[string]string{
		"host":     "*.*.*.*",
		"username": "root",
		"password": "******",
		"port":     "3306",
		"database": "***",
		"charset":  "utf8",
		"protocol": "tcp",
		"driver":   "mysql",
	},
}
var db gorose.Connection
var connectionError error

func init() {
	db, connectionError = gorose.Open(DbConfig)
	if connectionError != nil {
		panic(connectionError)
	}
}
func getDocument(url string) *goquery.Document {
	log.Printf("##################  URL => %s  ##################", url)
	doc, err := goquery.NewDocument(url)
	if err != nil {
		log.Fatal(err)
	}
	return doc
}

func getCatalogMap() map[string]map[string]string {
	catalogMap := map[string]string{
	// "家庭常用药": "https://www.yaofangwang.com/catalog-323.html",
	// "胃肠用药": "https://www.yaofangwang.com/Catalog-10.html",
	// "呼吸系统": "https://www.yaofangwang.com/Catalog-11.html",
	// "儿科用药": "https://www.yaofangwang.com/Catalog-20.html",
	// // "皮肤科药": "https://www.yaofangwang.com/Catalog-17.html",
	// "青少年": "https://www.yaofangwang.com/Catalog-50.html",
	// "药妆":  "https://www.yaofangwang.com/Catalog-51.html",
	// "五官科": "https://www.yaofangwang.com/Catalog-18.html",
	// "营养滋补": "https://www.yaofangwang.com/Catalog-30.html",
	// "心脑血管": "https://www.yaofangwang.com/Catalog-12.html",
	// "风湿骨科": "https://www.yaofangwang.com/Catalog-15.html",
	// "神经系统": "https://www.yaofangwang.com/Catalog-25.html",
	// "大众养生": "https://www.yaofangwang.com/Catalog-46.html",
	// "检测设备": "https://www.yaofangwang.com/Catalog-39.html",
	// "中医器械":  "https://www.yaofangwang.com/Catalog-40.html",
	// "中老年保健": "https://www.yaofangwang.com/Catalog-49.html",
	// "外用贴膏":  "https://www.yaofangwang.com/Catalog-43.html",
	// "民族用药":  "https://www.yaofangwang.com/Catalog-31.html",
	}
	subCatalogMap := make(map[string]map[string]string)
	for key, value := range catalogMap {
		//waitgroup.Add(1)
		log.Printf("爬SubCatalog  。。  %s , %s \n", key, value)
		//go func() {
		doc := getDocument(value)
		//适用症状
		doc.Find(".sitems li").First().Find("a").Each(func(i int, s *goquery.Selection) {
			href, _ := s.Attr("href")
			var subType string = s.Text()
			log.Printf(" %s => %s[%s] \n", key, subType, href)

			if subCatalogMap[key] == nil {
				subCatalogMap[key] = make(map[string]string)
			}
			subCatalogMap[key][subType] = "https://www.yaofangwang.com/" + href
		})
		//time.Sleep(10 * time.Second)
		//waitgroup.Done()
		//}()
	}
	//waitgroup.Wait()
	return subCatalogMap
}

func SubString(str string, begin, length int) (substr string) {
	// 将字符串的转换成[]rune
	rs := []rune(str)
	lth := len(rs)
	// 简单的越界判断
	if begin < 0 {
		begin = 0
	}
	if begin >= lth {
		begin = lth
	}
	end := begin + length
	if end > lth {
		end = lth
	}
	return string(rs[begin:end])
}

func saveGoods(goodsList *[]Goods) {
	log.Println("##################  基于gorose保存爬虫数据到数据库 。。。")
	for _, goods := range *goodsList {
		db.Table("A491382_Goods").Data(map[string]interface{}{
			"GoodsName": goods.GoodsName, "GoodsFactory": goods.GoodsFactory,
			"Spec": goods.Spec, "Price": goods.Price, "ImageUrl": goods.ImageUrl,
			"Catalog": goods.Catalog, "SubCatalog": goods.SubCatalog,
			"CommonName": goods.CommonName, "LicenseNumber": goods.LicenseNumber,
			"FitPeople": goods.FitPeople, "Shape": goods.Shape,
			"Brand": goods.Brand, "EnglishName": goods.EnglishName,
			"PinYinName": goods.PinYinName, "Standard": goods.Standard,
			"Looks": goods.Looks, "Theory": goods.Theory, "DetailUrl": goods.DetailUrl,
			"UsageRemark": goods.UsageRemark, "UntowardEffect": goods.UntowardEffect,
			"Taboo": goods.Taboo, "Attention": goods.Attention,
			"Store": goods.Store, "DrugInteractions": goods.DrugInteractions,
		}).Insert()

	}
	log.Println("##################  insert SQLs")
	for _, sql := range db.SqlLogs() {
		fmt.Println(sql)
	}
}

func pushGoodsToElasticsearch() {
	client, _ := elastic.NewClient(
		elastic.SetURL("http://127.0.0.1:9200"),
	)
	bulkRequest := client.Bulk()
	result, _ := db.Table("A491382_Goods").Get()
	for index, entity := range result {
		log.Printf("Row=> %d \n Entry => %v \n", index, entity)
		goodsId := strconv.FormatInt(entity["GoodsId"].(int64), 10)
		indexReq := elastic.NewBulkIndexRequest().Index("medicine").Type("details").Id(goodsId).Doc(entity)
		bulkRequest = bulkRequest.Add(indexReq)
	}
	bulkResponse, _ := bulkRequest.Do(context.TODO())

	log.Println(bulkResponse)
}

func main() {
	log.Println(" www.yaofangwang.com 爬虫 。。。 ")
	// goodsList := make([]Goods, 0)
	// subCatalogMap := getCatalogMap()
	// for catalog, subCatalogMap := range subCatalogMap {
	// 	for subcatalog, url := range subCatalogMap {
	// 		log.Printf("%s , %s , %s \n", catalog, subcatalog, url)
	// 		document := getDocument(url)
	// 		document.Find(".goodlist li").Each(func(i int, s *goquery.Selection) {
	// 			var price string = s.Find(".money").First().Text()
	// 			price = SubString(price, 1, len(price)-1)
	// 			var spec string = s.Find(".st").First().Text()
	// 			spec = SubString(spec, 3, len(spec)-1)
	// 			var name string = s.Find(".txt").First().Text()
	// 			detailUrl, _ := s.Find(".txt").First().Attr("href")
	// 			url, _ := s.Find(".autoimg").First().Attr("src")
	// 			var factory string = s.Find(".n").Last().Text()
	// 			factory = SubString(factory, 5, len(factory)-1)
	// 			log.Printf("name:%s , spec:%s , price:%s , url:%s , factory:%s \n",
	// 				name, spec, price, url, factory)

	// 			goods := Goods{GoodsName: name, GoodsFactory: factory, Spec: spec,
	// 				Price: price, ImageUrl: url, Catalog: catalog, SubCatalog: subcatalog,
	// 				DetailUrl: "https:" + detailUrl}
	// 			goodsList = append(goodsList, goods)
	// 		})
	// 	}
	// }
	// saveGoods(&goodsList)
	// log.Printf("爬虫共找到 %d 条记录", len(goodsList))

	log.Printf("############## 爬虫第二次爬取详细信息 ")
	for count := 0; count < 100; count++ {
		result, _ := db.Table("A491382_Goods").Where("CommonName", "=", "").Limit(10).Offset(count*10 + 1).Get()

		for _, entity := range result {
			log.Printf("# 药品名称 %s  , 详情URL %s ", entity["GoodsName"].(string), entity["DetailUrl"].(string))
			doc := getDocument(entity["DetailUrl"].(string))
			var goodsId = doc.Find(".ids").First().Text()
			goodsId = SubString(goodsId, 5, len(goodsId)-1)
			log.Printf("# 药品编号 %s ", goodsId)
			doc.Find(".maininfo .right dl dd").Each(func(i int, s *goquery.Selection) {
				switch i {
				case 1:
					entity["CommonName"] = s.Text()
					log.Printf("# CommonName %s ", entity["CommonName"])
				case 2:
					brand := s.Text()
					if len(brand) > 5 {
						entity["Brand"] = SubString(brand, 0, len(brand)-5)
					} else {
						entity["Brand"] = ""
					}
					log.Printf("# Brand %s ", entity["Brand"])
				case 4:
					entity["Shape"] = s.Text()
					log.Printf("# Shape %s ", entity["Shape"])
				case 7:
					entity["LicenseNumber"] = s.Text()
					log.Printf("# LicenseNumber %s ", entity["LicenseNumber"])
				case 8:
					entity["FitPeople"] = s.Find("strong").First().Text()
					log.Printf("# FitPeople %s ", entity["FitPeople"])
				}
			})
			doc.Find(".guidecontainer .subinfo dd").Each(func(i int, s *goquery.Selection) {
				switch i {
				case 1:
					entity["EnglishName"] = s.Text()
				case 2:
					entity["PinYinName"] = s.Text()
				}
			})
			doc.Find(".guidecontainer .maininfo  dd").Each(func(i int, s *goquery.Selection) {
				switch i {
				case 0:
					entity["Standard"] = s.Text()
				case 1:
					entity["Looks"] = s.Text()
				case 3:
					entity["UsageRemark"] = s.Text()
				case 6:
					entity["UntowardEffect"] = s.Text()
				case 7:
					entity["Taboo"] = s.Text()
				case 8:
					entity["Attention"] = s.Text()
				case 9:
					entity["DrugInteractions"] = s.Text()
				case 10:
					entity["Store"] = s.Text()
				}
			})
			db.Table("A491382_Goods").
				Data(map[string]interface{}{
					"FitPeople":        entity["FitPeople"],
					"CommonName":       entity["CommonName"],
					"Brand":            entity["Brand"],
					"Shape":            entity["Shape"],
					"EnglishName":      entity["EnglishName"],
					"PinYinName":       entity["PinYinName"],
					"Standard":         entity["Standard"],
					"Looks":            entity["Looks"],
					"UsageRemark":      entity["UsageRemark"],
					"UntowardEffect":   entity["UntowardEffect"],
					"Taboo":            entity["Taboo"],
					"Attention":        entity["Attention"],
					"DrugInteractions": entity["DrugInteractions"],
					"Store":            entity["Store"],
				}).Where("GoodsId", entity["GoodsId"].(int64)).Update()
			time.Sleep(2 * time.Second)
		}
		for _, sql := range db.SqlLogs() {
			fmt.Println(sql)
		}
	}
	//waitgroup.Wait()

	//pushGoodsToElasticsearch()
}
