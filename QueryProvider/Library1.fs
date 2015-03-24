#if INTERACTIVE
#else
module QueryExpression
#endif

open System
open System.Linq
open System.Linq.Expressions
open System.Reflection
open Microsoft.FSharp.Reflection

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

type ExprType = 
    | Member of MemberInfo * string
    | Method of MethodInfo * string

let (|MethodCall|_|) (e:Expression)   = 
    match e.NodeType, e with 
    | ExpressionType.Call, (:? MethodCallExpression as e) -> 
         Some (Method (e.Method, e.Method.Name), e.Arguments |> Seq.toList) 
    | _ -> None
  
let (|SpecificCall|_|) name (e:Expression) = 
    match e with
    | MethodCall(Method(_, mName) as m, args) when mName = name -> Some (m, args)
    | _ -> None
   
let (|MemberAccess|_|) (e:Expression) = 
    match e.NodeType, e with
    | ExpressionType.MemberAccess, (:? MemberExpression as me) -> 
        Some (Member (me.Member, me.Member.Name))
    | _, _ -> None

let (|NewObj|_|) (e:Expression) = 
    match e.NodeType, e with
    | ExpressionType.New, (:? NewExpression as ne) ->
        ne.Arguments 
        |> Seq.fold (fun s a -> 
             match a with
             | MemberAccess expr -> 
                 expr :: s
             | MethodCall (meth, _) -> 
                meth :: s
             | _ -> s
        ) ([])
        |> Some
    | _, _ -> None



let (|Quote|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Quote, (:? UnaryExpression as ce) ->  ce.Operand
    | _ -> e

type SqlOperator = 
    | Sum of string
    | Count of string
    | Column of string

type SqlExpr =
    | SelectExpr of (Type * SqlOperator) list
    | GroupByExpr of Type * SqlOperator
    | Empty

let (|Lambda|_|) (e:Expression) =
    match e.NodeType, e with 
    | ExpressionType.Lambda, (:? LambdaExpression as ce) ->
        match ce.Body with
        | MemberAccess expr -> 
            Some (ce.Body.Type, [expr])
        | NewObj expr  ->
            Some (ce.Body.Type, expr)
        | _ -> None
    | _ -> None

let foldExpressions state projs = 
    projs
    |> List.fold (fun s expr ->
        match expr with
        | Member(mi, name) -> (mi.DeclaringType, Column name) :: s
        | Method(mi, methodName) -> 
            match methodName with
            | "Count" -> (mi.DeclaringType, Count methodName) :: s
            | "Sum" -> (mi.DeclaringType, Count methodName) :: s
            | _ -> s
    ) state

let (|Select|_|) (e:Expression) =
    match e with
    | SpecificCall "Select" (expr, [source; (Quote (Lambda (retType, projs)))]) ->          
        Some (retType, foldExpressions [] projs)
    | _ -> None

let (|GroupBy|_|) (e:Expression) =
    match e with
    | SpecificCall "GroupBy" (expr, [source; (Quote (Lambda (retType, projs)))]) -> 
        let retType = e.Type.GenericTypeArguments.[0]         
        Some (retType, foldExpressions [] projs)
    | _ -> None


type QueryProvider(state, expressionWalker : SqlExpr list -> Expression -> IQueryable) = 
     interface IQueryProvider with
          member __.CreateQuery(e:Expression) : IQueryable = expressionWalker state e 
          member __.CreateQuery<'T>(e:Expression) : IQueryable<'T> = expressionWalker state e :?> IQueryable<'T>
          member __.Execute(e:Expression) : obj = printfn "Called execute"; Unchecked.defaultof<_>
          member __.Execute<'T>(e:Expression) : 'T = 
            let w = expressionWalker state e
            printfn "Called execute<T> %A" state;
            if typeof<'T>.GetInterfaces().Contains(typeof<Collections.IEnumerable>)
            then unbox<'T> <| box [Microsoft.FSharp.Linq.RuntimeHelpers.AnonymousObject<_,_>("colin", 31)]
            else Unchecked.defaultof<'T>

and Queryable(sqlExpr) =
    member x.Sql : SqlExpr list = sqlExpr    

and Queryable<'T>(sqlexpr, expressionWalker) =
     inherit Queryable(sqlexpr) 
     interface IQueryable<'T> with 
         member x.GetEnumerator() = 
            let iq = (x :> IQueryable)
            iq.Provider.Execute<seq<'T>>(iq.Expression).GetEnumerator()

     interface IQueryable with
         member x.Provider = new QueryProvider(sqlexpr, expressionWalker) :> IQueryProvider
         member x.Expression =  Expression.Constant(x,typeof<IQueryable<'T>>) :> Expression 
         member x.ElementType = typeof<'T>
         member x.GetEnumerator() = 
             let iq = (x :> IQueryable)
             (iq.Provider.Execute<Collections.IEnumerable>(iq.Expression)).GetEnumerator()

let rec expressionWalker<'a> state (e:Expression) = 

    let createQueryable returnType source = 
        let ty = typedefof<Queryable<_>>.MakeGenericType(returnType)
        ty.GetConstructors().[0].Invoke([|source; box expressionWalker|]) :?> IQueryable

    match e with
    | Select (retType, projs) ->   
        createQueryable [|retType|] (SelectExpr(projs) :: state)
    | GroupBy (retType, h :: _) ->
        createQueryable [|retType|] (GroupByExpr(h) :: state)
    | _ -> 
    //    let retType = e.Type.GenericTypeArguments.[0]
        createQueryable [|e.Type|] (Empty :: state)

         
let q = new Queryable<_>([], expressionWalker)


[<EntryPoint>]
let main(argv) = 
    let result =
        query {
            for student in q do
            groupBy student.Age into g
            select (g.Key, g.Count())
        } |> Seq.toArray
    
    printfn "%A" result
    Console.ReadLine() |> ignore
    0

    

