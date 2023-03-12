# mx

另一个 mysql ORM。基于[crud](http://github.com/shesuyo/crud)开发。致力于简单使用，性能高效。

## 操作系统支持

64位操作系统

## 需要注意

`Update()`函数只会更新一条数据  
`Delete()`函数则会删除所有数据  
`In()`函数，在传入数据为空数组的时候则不会进行任何操作，如需要操作则使用`MustIn()`函数  
