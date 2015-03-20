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
         Some (e.Arguments |> Seq.toList, Method (e.Method, e.Method.Name)) 
    | _ -> None
  
let (|SpecificCall|_|) name (e:Expression) = 
    match e with
    | MethodCall(args, (Method(_, mName) as m)) when mName = name -> Some (args, m)
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
             | MethodCall (_, meth) -> 
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

let (|Select|_|) (e:Expression) =
    match e with
    | SpecificCall "Select" (args, expr) ->
        match expr with
        | Member(mi, name) -> Some (mi.DeclaringType, Column name)
        | Method(mi, methodName) -> 
            match methodName with
            | "Count" -> Some(mi.DeclaringType, Count methodName)
            | "Sum" -> Some(mi.DeclaringType, Count methodName)
            | _ -> None
    | _ -> None

type QueryProvider(expressionWalker : Expression -> IQueryable) = 
     interface IQueryProvider with
          member __.CreateQuery(e:Expression) : IQueryable = expressionWalker e 
          member __.CreateQuery<'T>(e:Expression) : IQueryable<'T> = expressionWalker e :?> IQueryable<'T>
          member __.Execute(e:Expression) : obj = printfn "Called execute"; Unchecked.defaultof<_>
          member __.Execute<'T>(e:Expression) : 'T = 
            let w = expressionWalker e
            printfn "Called execute<T> %A" (w :?> Queryable<'T>).Sql; 
            
            if typeof<'T>.GetInterfaces().Contains(typeof<Collections.IEnumerable>)
            then unbox<'T> <| box [Microsoft.FSharp.Linq.RuntimeHelpers.AnonymousObject<_,_>("colin", 31)]
            else Unchecked.defaultof<'T>

and Queryable<'T>(sqlexpr, expressionWalker) =
     member x.Sql = sqlexpr      
     interface IQueryable<'T> with 
         member x.GetEnumerator() = 
            let iq = (x :> IQueryable)
            iq.Provider.Execute<seq<'T>>(iq.Expression).GetEnumerator()

     interface IQueryable with
         member x.Provider = new QueryProvider(expressionWalker) :> IQueryProvider
         member x.Expression =  Expression.Constant(x,typeof<IQueryable<'T>>) :> Expression 
         member x.ElementType = typeof<'T>
         member x.GetEnumerator() = 
             let iq = (x :> IQueryable)
             (iq.Provider.Execute<Collections.IEnumerable>(iq.Expression)).GetEnumerator()

let rec expressionWalker<'a> (e:Expression) = 

    let createQueryable returnType source = 
        let ty = typedefof<Queryable<_>>.MakeGenericType(returnType)
        ty.GetConstructors().[0].Invoke([|source; box expressionWalker|]) :?> IQueryable

    match e with
    | Select (retType, projs) ->    
        createQueryable [|retType|] (SelectExpr(projs))
    | Call(e, (Method "GroupBy" m), [source; (Quote (Lambda (_, h::_)))]) ->
        let retType = m.ReturnType.GenericTypeArguments.[0]
        createQueryable [|retType|] (GroupByExpr(h))
    | _ -> createQueryable [|e.Type|] Empty

         
let q = new Queryable<_>(Empty, expressionWalker)


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

    

