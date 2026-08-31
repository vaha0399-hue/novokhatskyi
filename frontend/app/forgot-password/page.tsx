import { FAAuthExperience } from "@/components/fa-auth-experience";
import { authPageMessage } from "@/lib/supabase/auth-messages";

export default async function ForgotPasswordPage({ searchParams }: { searchParams: Promise<{ error?: string }> }) {
  const message = authPageMessage((await searchParams).error);
  return <FAAuthExperience initialView="forgot" initialMessage={message} />;
}
