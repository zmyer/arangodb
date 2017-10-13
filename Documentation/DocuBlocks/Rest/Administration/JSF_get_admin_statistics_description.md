
@startDocuBlock JSF_get_admin_statistics_description
@brief fetch descriptive info of statistics

@RESTHEADER{GET /_admin/statistics-description, Statistics description}

@RESTDESCRIPTION

Returns a description of the statistics returned by */_admin/statistics*.
The returned objects contains an array of statistics groups in the attribute
*groups* and an array of statistics figures in the attribute *figures*.

@RESTRETURNCODES

@RESTRETURNCODE{200}
Description was returned successfully.

@RESTSTRUCT{description,JSF_statistics-description-group,string,required,}
A description of the group.
@RESTSTRUCT{name,JSF_statistics-description-group,string,required,}
The name of the group.
@RESTSTRUCT{group,JSF_statistics-description-group,string,required,}
The identifier of the group to which this figure belongs.

@RESTREPLYBODY{group,array,required,JSF_statistics-description-group}
an array of statistic groups

@RESTSTRUCT{identifier,JSF_statistics-figures,string,required,}
The identifier of the figure. It is unique within the group.
@RESTSTRUCT{name,JSF_statistics-figures,string,required,}
The name of the figure.
@RESTSTRUCT{description,JSF_statistics-figures,string,required,}
A description of the figure.
@RESTSTRUCT{type,JSF_statistics-figures,string,required,}
Either *current*, *accumulated*, or *distribution*.
@RESTSTRUCT{cuts,JSF_statistics-figures,array,optional,number}
The distribution vector.
@RESTSTRUCT{units,JSF_statistics-figures,string,required,}
Units in which the figure is measured.
@RESTSTRUCT{group,JSF_statistics-figures,string,required,}
The identifier of the group to which this figure belongs.

@RESTREPLYBODY{figures,array,required,JSF_statistics-figures}
One statistics element

@RESTREPLYBODY{error,boolean,required,}
Usually false.

@RESTREPLYBODY{code,integer,required,uint_64}
The return code - should be 200

@EXAMPLES

@EXAMPLE_ARANGOSH_RUN{RestAdminStatisticsDescription1}
    var url = "/_admin/statistics-description";
    var response = logCurlRequest('GET', url);

    assert(response.code === 200);

    logJsonResponse(response);
@END_EXAMPLE_ARANGOSH_RUN
@endDocuBlock

