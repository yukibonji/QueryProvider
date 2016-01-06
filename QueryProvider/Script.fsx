#load "Scripts/load-project-debug.fsx"

open QueryExpression

type Course = {
    CourseId : int
    CourseName : string
}

type Student = {
    StudentId : int
    Name : string
    Age : int
}

type CourseSelection = {
    Id : int
    StudentId : int
    CourseId : int
}

let students = [
    { StudentId = 1; Name = "Tom"; Age = 21 }
    { StudentId = 2; Name = "Dave"; Age = 21 }
    { StudentId = 3; Name = "Anna"; Age = 22 }
    { StudentId = 4; Name = "Sophie"; Age = 21 }
    { StudentId = 5; Name = "Richard"; Age = 20 }
]

let courses = [
    { CourseId = 1; CourseName = "Computer Science" }
    { CourseId = 2; CourseName = "Physics" }
]

let courseStudent = [
    { Id = 1; StudentId = 1; CourseId = 1; }
    { Id = 2; StudentId = 2; CourseId = 1; }
    { Id = 3; StudentId = 3; CourseId = 1; }
    { Id = 4; StudentId = 4; CourseId = 2; }
    { Id = 5; StudentId = 5; CourseId = 2; }
]

let studentsQ = new Queryable<Student>(Sql.ofExpression)
let courseStudentsQ = new Queryable<CourseSelection>(Sql.ofExpression)

let expr =
    query {
        for student in studentsQ do
        select student.Name
    } |> Seq.toArray  

