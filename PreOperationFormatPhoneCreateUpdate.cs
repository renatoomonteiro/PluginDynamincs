using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System.Text.RegularExpressions;
using System;
using System.Linq;

namespace PluginTestRide
{
    public class PreOperationFormatPhoneCreateUpdate : IPlugin
    {
        public void Execute(IServiceProvider serviceProvider)
        {
            var tracing = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
            var context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));

            try
            {
                // Evita recursão se plugin for chamado por update feito por este mesmo plugin
                if (context.Depth > 1)
                {
                    tracing?.Trace($"Depth {context.Depth} > 1: encerrando para evitar recursão.");
                    return;
                }

                // Valida Target
                if (!context.InputParameters.Contains("Target") || !(context.InputParameters["Target"] is Entity))
                {
                    tracing?.Trace("Target ausente ou inválido. Saindo.");
                    return;
                }

                var entity = (Entity)context.InputParameters["Target"];

                // Processa somente a entidade desejada
                if (!string.Equals(entity.LogicalName, "tr_dadospessoais", StringComparison.OrdinalIgnoreCase))
                {
                    tracing?.Trace($"Entidade diferente: {entity.LogicalName}. Saindo.");
                    return;
                }

                // Formata telefone se presente
                if (entity.Attributes.Contains("tr_telefonecliente"))
                {
                    var rawPhone = entity.GetAttributeValue<string>("tr_telefonecliente");
                    if (!string.IsNullOrWhiteSpace(rawPhone))
                    {
                        var formattedNumber = Regex.Replace(rawPhone, "[^\\d]", "");
                        entity["tr_telefonecliente"] = formattedNumber;
                        tracing?.Trace($"Telefone formatado: '{rawPhone}' -> '{formattedNumber}'");
                    }
                }

                var factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                var service = factory.CreateOrganizationService(context.UserId);

                // =========================
                // Validação e normalização CPF
                // =========================
                string cpf = null;
                bool cpfInTarget = false;
                if (entity.Attributes.Contains("tr_cpfdadospessoais"))
                {
                    cpf = entity.GetAttributeValue<string>("tr_cpfdadospessoais");
                    cpfInTarget = true;
                }
                else if (context.MessageName.Equals("Update", StringComparison.OrdinalIgnoreCase)
                         && context.PreEntityImages != null
                         && context.PreEntityImages.Contains("PreImage"))
                {
                    var pre = context.PreEntityImages["PreImage"];
                    if (pre != null && pre.Contains("tr_cpfdadospessoais"))
                        cpf = pre.GetAttributeValue<string>("tr_cpfdadospessoais");
                }

                if (!string.IsNullOrWhiteSpace(cpf))
                {
                    var cpfNorm = NormalizeDigits(cpf);

                    // Se o CPF veio no Target, sobrescreve o Target com a versão normalizada (apenas dígitos)
                    if (cpfInTarget && !string.IsNullOrEmpty(cpfNorm))
                    {
                        entity["tr_cpfdadospessoais"] = cpfNorm;
                        tracing?.Trace($"CPF normalizado e atribuído ao Target: '{cpf}' -> '{cpfNorm}'");
                    }

                    var q = new QueryExpression("tr_dadospessoais")
                    {
                        ColumnSet = new ColumnSet("tr_cpfdadospessoais"),
                        TopCount = 1
                    };

                    var or = new FilterExpression(LogicalOperator.Or);

                    // Procura por igualdade com o valor normalizado (preferencial) e também com o original (caso já exista formatado)
                    if (!string.IsNullOrEmpty(cpfNorm))
                        or.AddCondition("tr_cpfdadospessoais", ConditionOperator.Equal, cpfNorm);

                    if (!string.IsNullOrEmpty(cpf) && cpfNorm != cpf)
                        or.AddCondition("tr_cpfdadospessoais", ConditionOperator.Equal, cpf);

                    // Se não houver nenhuma condição válida, não prossegue
                    if (or.Conditions.Count == 0)
                    {
                        tracing?.Trace("Nenhuma condição válida para busca de CPF. Saindo.");
                    }
                    else
                    {
                        q.Criteria.AddFilter(or);

                        if (context.MessageName.Equals("Update", StringComparison.OrdinalIgnoreCase) && context.PrimaryEntityId != Guid.Empty)
                        {
                            q.Criteria.AddCondition("tr_dadospessoaisid", ConditionOperator.NotEqual, context.PrimaryEntityId);
                        }

                        var existing = service.RetrieveMultiple(q);
                        if (existing.Entities != null && existing.Entities.Any())
                        {
                            tracing?.Trace("Duplicata de CPF encontrada. Abortando operação.");
                            throw new InvalidPluginExecutionException("Já há um usuário cadastrado com esse CPF");
                        }

                        // Se CPF não veio no Target (Update sem alteração do campo), atualiza o registro com CPF normalizado
                        if (!cpfInTarget
                            && context.MessageName.Equals("Update", StringComparison.OrdinalIgnoreCase)
                            && context.PrimaryEntityId != Guid.Empty
                            && !string.IsNullOrEmpty(cpfNorm))
                        {
                            var toUpdate = new Entity("tr_dadospessoais") { Id = context.PrimaryEntityId };
                            toUpdate["tr_cpfdadospessoais"] = cpfNorm;
                            service.Update(toUpdate);
                            tracing?.Trace($"CPF normalizado via Update automático: '{cpf}' -> '{cpfNorm}'");
                        }
                    }
                }

                // =========================
                // Validação e normalização RG
                // =========================
                string rg = null;
                bool rgInTarget = false;
                if (entity.Attributes.Contains("tr_rgdadospessoais"))
                {
                    rg = entity.GetAttributeValue<string>("tr_rgdadospessoais");
                    rgInTarget = true;
                }
                else if (context.MessageName.Equals("Update", StringComparison.OrdinalIgnoreCase)
                         && context.PreEntityImages != null
                         && context.PreEntityImages.Contains("PreImage"))
                {
                    var preRg = context.PreEntityImages["PreImage"];
                    if (preRg != null && preRg.Contains("tr_rgdadospessoais"))
                        rg = preRg.GetAttributeValue<string>("tr_rgdadospessoais");
                }

                if (!string.IsNullOrWhiteSpace(rg))
                {
                    var rgNorm = NormalizeDigits(rg);

                    // Se o RG veio no Target, sobrescreve o Target com a versão normalizada (apenas dígitos)
                    if (rgInTarget && !string.IsNullOrEmpty(rgNorm))
                    {
                        entity["tr_rgdadospessoais"] = rgNorm;
                        tracing?.Trace($"RG normalizado e atribuído ao Target: '{rg}' -> '{rgNorm}'");
                    }

                    var qRg = new QueryExpression("tr_dadospessoais")
                    {
                        ColumnSet = new ColumnSet("tr_rgdadospessoais"),
                        TopCount = 1
                    };

                    var orRg = new FilterExpression(LogicalOperator.Or);
                    if (!string.IsNullOrEmpty(rgNorm))
                        orRg.AddCondition("tr_rgdadospessoais", ConditionOperator.Equal, rgNorm);
                    if (!string.IsNullOrEmpty(rg) && rgNorm != rg)
                        orRg.AddCondition("tr_rgdadospessoais", ConditionOperator.Equal, rg);

                    if (orRg.Conditions.Count > 0)
                    {
                        qRg.Criteria.AddFilter(orRg);

                        if (context.MessageName.Equals("Update", StringComparison.OrdinalIgnoreCase) && context.PrimaryEntityId != Guid.Empty)
                        {
                            qRg.Criteria.AddCondition("tr_dadospessoaisid", ConditionOperator.NotEqual, context.PrimaryEntityId);
                        }

                        var existingRg = service.RetrieveMultiple(qRg);
                        if (existingRg.Entities != null && existingRg.Entities.Any())
                        {
                            tracing?.Trace("Duplicata de RG encontrada. Abortando operação.");
                            throw new InvalidPluginExecutionException("Já há um usuário cadastrado com esse RG");
                        }

                        // Se RG não veio no Target (Update sem alteração do campo), atualiza o registro com RG normalizado
                        if (!rgInTarget
                            && context.MessageName.Equals("Update", StringComparison.OrdinalIgnoreCase)
                            && context.PrimaryEntityId != Guid.Empty
                            && !string.IsNullOrEmpty(rgNorm))
                        {
                            var toUpdateRg = new Entity("tr_dadospessoais") { Id = context.PrimaryEntityId };
                            toUpdateRg["tr_rgdadospessoais"] = rgNorm;
                            service.Update(toUpdateRg);
                            tracing?.Trace($"RG normalizado via Update automático: '{rg}' -> '{rgNorm}'");
                        }
                    }
                }

                // =========================
                // Validação e normalização CNH
                // =========================
                string cnh = null;
                bool cnhInTarget = false;
                if (entity.Attributes.Contains("tr_cnhdadospessoais"))
                {
                    cnh = entity.GetAttributeValue<string>("tr_cnhdadospessoais");
                    cnhInTarget = true;
                }
                else if (context.MessageName.Equals("Update", StringComparison.OrdinalIgnoreCase)
                         && context.PreEntityImages != null
                         && context.PreEntityImages.Contains("PreImage"))
                {
                    var preCnh = context.PreEntityImages["PreImage"];
                    if (preCnh != null && preCnh.Contains("tr_cnhdadospessoais"))
                        cnh = preCnh.GetAttributeValue<string>("tr_cnhdadospessoais");
                }

                if (!string.IsNullOrWhiteSpace(cnh))
                {
                    // Normaliza CNH removendo quaisquer caracteres não numéricos (segurança contra formatação futura)
                    var cnhNorm = NormalizeDigits(cnh);

                    // Se CNH veio no Target, sobrescreve o Target com a versão normalizada (apenas dígitos)
                    if (cnhInTarget && !string.IsNullOrEmpty(cnhNorm))
                    {
                        entity["tr_cnhdadospessoais"] = cnhNorm;
                        tracing?.Trace($"CNH normalizada e atribuída ao Target: '{cnh}' -> '{cnhNorm}'");
                    }

                    var qCnh = new QueryExpression("tr_dadospessoais")
                    {
                        ColumnSet = new ColumnSet("tr_cnhdadospessoais"),
                        TopCount = 1
                    };

                    var orCnh = new FilterExpression(LogicalOperator.Or);
                    // compara normalizado preferencialmente e também o original caso exista com máscara
                    if (!string.IsNullOrEmpty(cnhNorm))
                        orCnh.AddCondition("tr_cnhdadospessoais", ConditionOperator.Equal, cnhNorm);
                    if (!string.IsNullOrEmpty(cnh) && cnhNorm != cnh)
                        orCnh.AddCondition("tr_cnhdadospessoais", ConditionOperator.Equal, cnh);

                    if (orCnh.Conditions.Count > 0)
                    {
                        qCnh.Criteria.AddFilter(orCnh);

                        if (context.MessageName.Equals("Update", StringComparison.OrdinalIgnoreCase) && context.PrimaryEntityId != Guid.Empty)
                        {
                            qCnh.Criteria.AddCondition("tr_dadospessoaisid", ConditionOperator.NotEqual, context.PrimaryEntityId);
                        }

                        var existingCnh = service.RetrieveMultiple(qCnh);
                        if (existingCnh.Entities != null && existingCnh.Entities.Any())
                        {
                            tracing?.Trace("Duplicata de CNH encontrada. Abortando operação.");
                            throw new InvalidPluginExecutionException("Já há um usuário cadastrado com essa CNH");
                        }

                        // Se CNH não veio no Target (Update sem alteração do campo), atualiza o registro com CNH normalizado
                        if (!cnhInTarget
                            && context.MessageName.Equals("Update", StringComparison.OrdinalIgnoreCase)
                            && context.PrimaryEntityId != Guid.Empty
                            && !string.IsNullOrEmpty(cnhNorm))
                        {
                            var toUpdateCnh = new Entity("tr_dadospessoais") { Id = context.PrimaryEntityId };
                            toUpdateCnh["tr_cnhdadospessoais"] = cnhNorm;
                            service.Update(toUpdateCnh);
                            tracing?.Trace($"CNH normalizada via Update automático: '{cnh}' -> '{cnhNorm}'");
                        }
                    }
                }
            }
            catch (InvalidPluginExecutionException)
            {
                throw;
            }
            catch (Exception ex)
            {
                tracing?.Trace("Exceção no plugin PreOperationFormatPhoneCreateUpdate: " + ex.ToString());
                throw new InvalidPluginExecutionException("Erro ao processar validações: " + ex.Message);
            }
        }

        private static string NormalizeDigits(string input)
        {
            if (string.IsNullOrWhiteSpace(input)) return null;
            return new string(input.Where(char.IsDigit).ToArray());
        }
    }
}