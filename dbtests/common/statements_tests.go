package common

import (
	"testing"

	"gitlab.com/samonzeweb/godb"
)

func StatementsTests(db *godb.DB, t *testing.T) {
	// Enable logger if needed
	//db.SetLogger(log.New(os.Stderr, "", 0))

	statementInsertTest(db, t)
	statementSelectTest(db, t)
	statementUpdateTest(db, t)
	statementDeleteTest(db, t)
}

func statementInsertTest(db *godb.DB, t *testing.T) {
	// Simple insert
	query := db.InsertInto("books").
		Columns("title", "author", "published").
		Values(bookTheHobbit.Title, bookTheHobbit.Author, bookTheHobbit.Published)

	id, err := query.Do()
	if err != nil {
		t.Fatal(err)
	}
	if id == 0 && db.Adapter().DriverName() != "postgres" {
		t.Fatal("Id was not returned.")
	}

	// Multiple insert
	booksToInsert := setTheLordOfTheRing[:]
	withReturning := db.Adapter().DriverName() == "postgres"
	if !withReturning {
		booksToInsert = append(booksToInsert, setFoundation...)
	}
	query = db.InsertInto("books").
		Columns("title", "author", "published")
	for _, book := range booksToInsert {
		query.Values(book.Title, book.Author, book.Published)
	}
	_, err = query.Do()
	if err != nil {
		t.Fatal(err)
	}

	// Multiple insert with returning statements (only postgreSQL)
	if withReturning {
		booksToInsert = setFoundation[:]

		query = db.InsertInto("books").
			Columns("title", "author", "published").
			Suffix("RETURNING id")
		for _, book := range booksToInsert {
			query.Values(book.Title, book.Author, book.Published)
		}
		err = query.DoWithReturning(&booksToInsert)
		if err != nil {
			t.Fatal(err)
		}
		for _, book := range booksToInsert {
			if book.Id == 0 {
				t.Fatalf("Id not set, fail go get returning values : %v", book)
			}
		}
	}
}

func statementSelectTest(db *godb.DB, t *testing.T) {
	// Count books
	count, err := db.SelectFrom("books").Count()
	if err != nil {
		t.Fatal(err)
	}
	if count != 7 {
		t.Fatalf("Wrong books count : %v", count)
	}

	// Select a single row
	book := Book{}
	err = db.SelectFrom("books").
		Columns("id", "title", "author", "published").
		Where("title = ?", bookTheHobbit.Title).Do(&book)
	if err != nil {
		t.Fatal(err)
	}
	if book.Title != bookTheHobbit.Title {
		t.Fatalf("Book not filled : %v", book)
	}

	// Select multiple rows with order
	allBooks := make([]Book, 0, 0)
	err = db.SelectFrom("books").
		Columns("id", "title", "author", "published").
		OrderBy("author").OrderBy("title").
		Do(&allBooks)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(allBooks)) != count {
		t.Fatalf("Wrong books count : %v", len(allBooks))
	}
	if allBooks[0].Title != bookFoundation.Title {
		t.Fatalf("Wrong book order first is : %v", allBooks[0])
	}

	// Select with group by and having
	countByAuthor := make([]CountByAuthor, 0, 0)
	err = db.SelectFrom("books").
		Columns("author", "count(*) as count").
		GroupBy("author").
		Having("count(*) > 3").
		Do(&countByAuthor)
	if err != nil {
		t.Fatal(err)
	}
	if len(countByAuthor) != 1 {
		t.Fatalf("Wrong count by author, total rows is : %v", len(countByAuthor))
	}
	if countByAuthor[0].Author != authorTolkien ||
		countByAuthor[0].Count != 4 {
		t.Fatalf("Wrong result : %v", countByAuthor[0])
	}

	// Select with complex condition
	titles := []string{
		bookFoundation.Title,
		bookFoundationAndEmpire.Title,
	}
	q := godb.And(
		godb.Q("author = ?", authorAssimov),
		godb.Q("title in (?)", titles),
	)
	twoBooks := make([]Book, 0, 0)
	err = db.SelectFrom("books").
		Columns("id", "title", "author", "published").
		WhereQ(q).
		Do(&twoBooks)
	if err != nil {
		t.Fatal(err)
	}
	if len(twoBooks) != 2 {
		t.Fatalf("Wrong result, books count : %v", len(twoBooks))
	}
}

func statementUpdateTest(db *godb.DB, t *testing.T) {
	// Update books
	gandalf := "Gandalf the Grey"
	updated, err := db.UpdateTable("books").
		Set("author", gandalf).
		SetRaw("title = 'book by Gandalf'").
		Where("author = ?", authorTolkien).
		Do()
	if err != nil {
		t.Fatal(err)
	}
	if updated != 4 {
		t.Fatalf("Wrong count of updated books : %v", updated)
	}

	// Count changed
	count, err := db.SelectFrom("books").
		Where("author = ?", gandalf).
		Where("title = 'book by Gandalf'").
		Count()
	if err != nil {
		t.Fatal(err)
	}
	if count != updated {
		t.Fatalf("Wrong books count : %v", count)
	}
}

func statementDeleteTest(db *godb.DB, t *testing.T) {
	deleted, err := db.DeleteFrom("books").
		Where("author = ?", authorAssimov).
		Do()
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 3 {
		t.Fatalf("Wrong count of deleted books : %v", deleted)
	}
	// Count books
	count, err := db.SelectFrom("books").Count()
	if err != nil {
		t.Fatal(err)
	}
	if count != 4 {
		t.Fatalf("Wrong books count : %v", count)
	}
}
