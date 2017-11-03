package godb

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestInsertDo(t *testing.T) {
	Convey("Given a test database", t, func() {
		db := fixturesSetup(t)
		defer db.Close()

		Convey("Given an object to insert", func() {
			dummy := Dummy{
				AText:       "Foo Bar",
				AnotherText: "Baz",
				AnInteger:   1234,
			}

			Convey("Do execute the query and fill the auto key", func() {
				err := db.Insert(&dummy).Do()

				So(err, ShouldBeNil)
				So(dummy.ID, ShouldBeGreaterThan, 0)

				Convey("The data are in the database", func() {
					retrieveddummy := Dummy{}
					db.Select(&retrieveddummy).Where("id = ?", dummy.ID).Do()
					So(retrieveddummy.ID, ShouldEqual, dummy.ID)
					So(retrieveddummy.AText, ShouldEqual, dummy.AText)
					So(retrieveddummy.AnotherText, ShouldEqual, dummy.AnotherText)
					So(retrieveddummy.AnInteger, ShouldEqual, dummy.AnInteger)
				})
			})
		})
	})
}

func TestBulkInsertDo(t *testing.T) {
	Convey("Given a test database", t, func() {
		db := fixturesSetup(t)
		defer db.Close()

		Convey("Given a slice of objects to insert", func() {
			slice := make([]Dummy, 0, 0)
			for i := 1; i <= 10; i++ {
				dummy := Dummy{
					AText:       "Bulk",
					AnotherText: "Insert",
					AnInteger:   i * 100,
				}
				slice = append(slice, dummy)
			}

			Convey("Do execute the query", func() {
				err := db.BulkInsert(&slice).Do()
				So(err, ShouldBeNil)

				Convey("The data are in the database", func() {
					retrieveddummies := make([]Dummy, 0, 0)
					db.Select(&retrieveddummies).
						Where("an_integer > 99").
						Where("a_text = ?", "Bulk").
						Do()
					So(len(retrieveddummies), ShouldEqual, 10)
				})
			})
		})
	})
}
